#! /usr/bin/perl
###
### Native MultiThreaded (Gidra) Daemon Galera Cluster Monitor
### Andrey Kislyak
### 21 Sep 2017
### ===============================================================
### Create Galera User
### mysql> GRANT USAGE ON *.* TO 'monitor'@'%' IDENTIFIED BY 'monitor';
### mysql> FLUSH PRIVILEGES;
### ===============================================================
##  INSERT INTO mysql_servers(hostgroup_id,hostname,    port,weight, max_connections,comment)
##                    VALUES (10,         '10.5.47.109',3306,1000000,1000,           'WRITE'),
##                           (11,'10.5.47.189',3306,1000,1000,'READ');
##
                                                                              
##  INSERT INTO mysql_query_rules(rule_id,active,username,match_digest,destination_hostgroup,apply)
##                        VALUES (1,1,'testuser','^SELECT.*FOR UPDATE','10',1),
##                               (2,1,'testuser','^SELECT','11',1);

##                                                                                                             
##  INSERT INTO mysql_users(username,password,active,default_hostgroup,transaction_persistent,backend,frontend,max_connections)
##                   VALUES('testuser','secretpass',1,10,1,1,1,10000);

use strict;
use warnings;
use POSIX;
use threads;
use threads::shared;
use DBI;


my @mysql_status:shared;
my @threads;
my $proxysql_user='admin';
my $proxysql_pass='admin';
my $proxysql_host='miki';
my $proxysql_port='6032';
my $proxysql_wrid='10';
my $proxysql_rdid='11';
my $mysql_monitor_user='monitor';
my $mysql_monitor_pass='monitor';
my @mysql_servers = ( ['centos01',3306],
                      ['centos02',3306],
                      ['centos03',3306],
                    );
my $AVAILABLE_WHEN_DONOR=1;
my $log_file = '/tmp/proxy-gidra.log';
my $pid_file = '/tmp/proxy-gidra.pid';

### Timeouts                    
my $main_loop_timeout=15;
my $mysql_err_timeout=5;
my $mysql_monitor_timeout=5;
my $proxy_suspend_timeout=30;


$SIG{INT} = $SIG{TERM} = sub { unlink($pid_file); die "fd exited\n"; };
if(-e $pid_file){ die "$pid_file exists.\n"; }

daemonize();
write_pid();
main();
exit 0;
   

sub main {
 my @thr;
 for (my $i=1;$i<=scalar @mysql_servers; $i++) { ## Run thread for earch mysql server
     $thr[$i] = threads->create('thread_mysql_status', $i);
     for (my $j=0;$j<3;$j++) {
     # Apply magic number to flat array (Last change time, Last check time, Status)
         $mysql_status[($i-1)*3+$j]= 13; 
     } 
 }
 # Wait before start proxysql_thread for colect some statistics
 sleep $mysql_monitor_timeout*2;
 $thr[0] = threads->create('thread_proxysql_admin');
 while (1) {
  #Monitor threads
  sleep $main_loop_timeout;
 }
}
sub write_pid {
  open(PID, ">$pid_file") or die "fd: can't open $pid_file: $!\n";
  print PID $$;
  close PID;
}
sub mylog ($){
  my $message=shift;
  open(LOG, ">>$log_file") or die "fd: can't open $log_file: $!\n";
  print LOG getLoggingTime()." ".$message."\n" ;
  close LOG;
}
sub getLoggingTime {
  my ($sec,$min,$hour,$mday,$mon,$year,$wday,$yday,$isdst)=localtime(time);
  my $nice_timestamp = sprintf ( "%04d.%02d.%02d %02d:%02d:%02d",$year+1900,$mon+1,$mday,$hour,$min,$sec);
  return $nice_timestamp;
}
sub daemonize {
  POSIX::setsid or die "setsid: $!";
  my $pid = fork() // die $!; #//
  exit(0) if $pid;

  chdir "/";
  umask 0;
  for (0 .. (POSIX::sysconf (&POSIX::_SC_OPEN_MAX) || 1024))
      { POSIX::close $_ }
  open (STDIN, "</dev/null");
  open (STDOUT, ">/dev/null");
  open (STDERR, ">&STDOUT");
 }
   
sub thread_mysql_status {
  my ($server_number) = @_ ;
  my $database="information_schema";
  my $dsn="DBI:mysql:database=$database;host=$mysql_servers[$server_number-1][0];port=$mysql_servers[$server_number-1][1];mysql_connect_timeout=10;mysql_write_timeout=5;mysql_read_timeout=5";
  my %attr = ( PrintError => 0, RaiseError => 0 );
  
  while ( 1 ) {   ### Start main thread loop
    my $dbh;
    mylog("Thr:$server_number HOST:$mysql_servers[$server_number-1][0]_$mysql_servers[$server_number-1][1] OK:Started");
    until ( $dbh = DBI->connect( $dsn, $mysql_monitor_user, $mysql_monitor_pass, \%attr ) ) {
      mylog("Thread: $server_number HOST:$mysql_servers[$server_number-1][0]:$mysql_servers[$server_number-1][1] Err:Can't connect: $DBI::errstr. Pausing before retrying.");
      sleep $mysql_err_timeout;
    }
    eval {      ### Catch _any_ kind of failures from the code within
      $dbh->{RaiseError} = 1;
      my $sth_wcs = $dbh->prepare( "SHOW STATUS LIKE 'wsrep_cluster_status'" );
      my $sth_wls = $dbh->prepare( "SHOW STATUS LIKE 'wsrep_local_state'" );
      my $sth_pmm = $dbh->prepare( "SHOW VARIABLES LIKE 'pxc_maint_mode'" );
      while (1) { ## Start monitoring thread loop
        $sth_wcs->execute( );
        $sth_wls->execute( );
        $sth_pmm->execute( );
        my $clusterstatus;
        my $nodestatus;
        my $maintemode;
        while ( my @row = $sth_wcs->fetchrow_array( ) )  { 
          if ($row[1] eq 'Primary') { $clusterstatus=1; } 
               else { $clusterstatus=0; }
        }
        while ( my @row = $sth_wls->fetchrow_array( ) )  {
               $nodestatus=$row[1]; 
        }
        while ( my @row = $sth_pmm->fetchrow_array( ) )  { 
          if ($row[1] eq 'DISABLED') { $maintemode=0;}
                 else { $maintemode=1; } 
        }
        my $nowsta;  # Status now
        if ( ( $nodestatus == 4 || ($nodestatus == 2 && $AVAILABLE_WHEN_DONOR == 1 )) && $clusterstatus == 1 ) { $nowsta = 0; }
        if ( $maintemode == 1 ) { $nowsta=1; }
        if ( ( $nodestatus != 4 && $nodestatus != 2 ) || $clusterstatus != 1 ) { $nowsta = 3; }
        my $time=time();    
        if ( $mysql_status[($server_number-1)*3+2] == $nowsta ) { # If status not changed
             $mysql_status[($server_number-1)*3+1] = $time;
        }
        else { #If status changed
          $mysql_status[($server_number-1)*3+0] = $time;
          $mysql_status[($server_number-1)*3+1] = $time;
          $mysql_status[($server_number-1)*3+2] = $nowsta;
        }
        mylog("Thr:$server_number Host:$mysql_servers[$server_number-1][0]_$mysql_servers[$server_number-1][1] STATUS FS:$nowsta NS:$nodestatus CS:$clusterstatus MA:$maintemode");
        sleep $mysql_monitor_timeout;
      }
    };
    mylog("Thr:$server_number Host:$mysql_servers[$server_number-1][0]:$mysql_servers[$server_number-1][1] Err:$@") if $@;
    my $nowsta=2;
    my $time=time(); 
    if ( $mysql_status[($server_number-1)*3+2] == $nowsta ) { # If status not changed
         $mysql_status[($server_number-1)*3+1] = $time;
    }
    else { #If status changed
         $mysql_status[($server_number-1)*3+0] = $time;
         $mysql_status[($server_number-1)*3+1] = $time;
         $mysql_status[($server_number-1)*3+2] = $nowsta;
    }
    mylog("Thr:$server_number Host:$mysql_servers[$server_number-1][0]_$mysql_servers[$server_number-1][1] STATUS FS:$nowsta NS:-1 CS:-1 MA:-1");
    sleep $mysql_err_timeout;
  }
  exit; ### Never running
}
sub read_proxy_conf($$$)
{
  my $dbh=shift;
  my $HASHref=shift;
  my $CALCref=shift;
  my @nodes_need_update=();
  my $sth_prx = $dbh->prepare( "SELECT hostgroup_id,hostname,port,status,comment FROM mysql_servers WHERE hostgroup_id=?" );
  $sth_prx->execute( $proxysql_wrid );
  while ( my @row = $sth_prx->fetchrow_array(  ) ) { #Read ProxySQL configuration
    my $tmp_node_name="$row[1]_$row[2]";
    my ($sta,$time)=split(':',$row[4]);
    my $set_time=time();
    $HASHref->{$tmp_node_name}->{'proxy_write_status'} = 2 ;
    if ( $row[3] eq 'ONLINE' )       { $HASHref->{$tmp_node_name}->{'proxy_write_status'} = 0; }
    if ( $row[3] eq 'OFFLINE_SOFT' ) { $HASHref->{$tmp_node_name}->{'proxy_write_status'} = 1; }
    if ( $row[3] eq 'OFFLINE_HARD' ) { $HASHref->{$tmp_node_name}->{'proxy_write_status'} = 3; }
    $HASHref->{$tmp_node_name}->{'proxy_write'}  = 1;
    if ( ( $sta eq 'SU' ) || ( $sta eq 'RD' ) || ( $sta eq 'WR' ) ) { 
      $HASHref->{$tmp_node_name}->{'proxy_write_change_time'}  = $time; 
    } else {
      $HASHref->{$tmp_node_name}->{'proxy_write_change_time'}  = time();
      push (@nodes_need_update, $tmp_node_name)
    }
    if ( ! defined $HASHref->{$tmp_node_name}->{'proxy_read'} )  { $HASHref->{$tmp_node_name}->{'proxy_read'}  = 0; }
    if ( ! defined $HASHref->{$tmp_node_name}->{'node_status'} ) { $HASHref->{$tmp_node_name}->{'node_status'}  = 2; }
  }
  $sth_prx->finish();
 
  foreach my $tmp_node (@nodes_need_update) {
    my $time=time();
    my ($host, $port) = split ('_',$tmp_node);
    mylog("Prx:0 Host:${proxysql_host}_${proxysql_port} ACTION:SU $tmp_node $proxysql_wrid");
    $dbh->do("UPDATE mysql_servers SET comment='SU:$time' WHERE hostname='$host' AND port='$port' AND hostgroup_id='$proxysql_wrid'")
  }
  @nodes_need_update=();
  $sth_prx->execute( $proxysql_rdid );
  while ( my @row = $sth_prx->fetchrow_array(  ) ) { #Read ProxySQL configuration
    my $tmp_node_name="$row[1]_$row[2]";
    my ($sta,$time)=split(':',$row[4]);
    $HASHref->{$tmp_node_name}->{'proxy_read_status'} = 2 ;
    if ( $row[3] eq 'ONLINE' )       { $HASHref->{$tmp_node_name}->{'proxy_read_status'} = 0; }
    if ( $row[3] eq 'OFFLINE_SOFT' ) { $HASHref->{$tmp_node_name}->{'proxy_read_status'} = 1; }
    if ( $row[3] eq 'OFFLINE_HARD' ) { $HASHref->{$tmp_node_name}->{'proxy_read_status'} = 3; }
    $HASHref->{$tmp_node_name}->{'proxy_read'}  = 1;
    if ( ( $sta eq "SU" ) || ( $sta eq "RD" ) || ( $sta eq "WR" ) ) { 
      $HASHref->{$tmp_node_name}->{'proxy_read_change_time'}  = $time; 
    } else { 
      $HASHref->{$tmp_node_name}->{'proxy_read_change_time'}  = time();
      push (@nodes_need_update, $tmp_node_name)
    }
    if ( ! defined $HASHref->{$tmp_node_name}->{'proxy_write'} )  { $HASHref->{$tmp_node_name}->{'proxy_write'}  = 0; }
    if ( ! defined $HASHref->{$tmp_node_name}->{'node_status'} ) { $HASHref->{$tmp_node_name}->{'node_status'}  = 2; }
  }
  $sth_prx->finish();
  foreach my $tmp_node (@nodes_need_update) {
    my $time=time();
    my ($host, $port) = split ('_',$tmp_node);
    mylog("Prx:0 Host:${proxysql_host}_${proxysql_port} ACTION:SU  $tmp_node $proxysql_rdid");
    $dbh->do("UPDATE mysql_servers SET comment='SU:$time' WHERE hostname='$host' AND port='$port' AND hostgroup_id='$proxysql_rdid'")
  }
  @{$CALCref->{'proxy_good_write_nodes_ref'}}=();
  @{$CALCref->{'proxy_bad_write_nodes_ref'}}=();
  @{$CALCref->{'proxy_good_read_nodes_ref'}}=();
  @{$CALCref->{'proxy_bad_read_nodes_ref'}}=();
  foreach my $node (keys %{$HASHref}) {     
    if ($HASHref->{$node}->{'proxy_write'} == 1) {
      if ($HASHref->{$node}->{'proxy_write_status'} == 0) { push (@{$CALCref->{'proxy_good_write_nodes_ref'}}, $node); }
        else { push (@{$CALCref->{'proxy_bad_write_nodes_ref'}}, "$node") ; }
    }
    if ($HASHref->{$node}->{'proxy_read'} == 1) {
      if ($HASHref->{$node}->{'proxy_read_status'} == 0) { push (@{$CALCref->{'proxy_good_read_nodes_ref'}}, "$node"); }
        else { push (@{$CALCref->{'proxy_bad_read_nodes_ref'}}, "$node") ; }
    }
  }
}
sub read_mysql_conf ($$) {
  my $HASHref=shift;
  my $CALCref=shift;
  for (my $i=0;$i<scalar @mysql_servers;$i++) { # Prepare HASH logic
    my $node="$mysql_servers[$i][0]_$mysql_servers[$i][1]";
    if ( !defined $HASHref->{$node}->{'proxy_write'} ) { $HASHref->{$node}->{'proxy_write'} = 0; }
    if ( !defined $HASHref->{$node}->{'proxy_read'} )  { $HASHref->{$node}->{'proxy_read'}  = 0; }
    $HASHref->{$node}->{'node_sta_chge'} = $mysql_status[$i*3+0];
    $HASHref->{$node}->{'node_sta_last'} = $mysql_status[$i*3+1];
    $HASHref->{$node}->{'node_status'}   = $mysql_status[$i*3+2];
    $HASHref->{$node}->{'user_port'}     = $mysql_servers[$i][1];
    if ( defined $mysql_servers[$i][2] ) { 
      $HASHref->{$node}{'adm_port'}  = $mysql_servers[$i][2];
    } else {
      $HASHref->{$node}->{'adm_port'}  = $mysql_servers[$i][1];
    }
  }
  @{$CALCref->{'mysql_good_nodes_ref'}}=();
  @{$CALCref->{'mysql_bad_nodes_ref'}}=();
  foreach my $node (keys %{$HASHref}) {
    if ($HASHref->{$node}->{'node_status'} == 0) { push (@{$CALCref->{'mysql_good_nodes_ref'}}, "$node"); }
      else { push (@{$CALCref->{'mysql_bad_nodes_ref'}}, "$node") ; }
  }
}
sub proxy_flush($) { 
  my $dbh=shift;
  mylog("Prx:0 Host:${proxysql_host}_${proxysql_port} ACTION:FLUSH");
  $dbh->do("LOAD MYSQL SERVERS TO RUNTIME");
  $dbh->do("SAVE MYSQL SERVERS TO DISK");
  return 0;
}
sub proxy_suspend_node($$$) { 
  my $dbh=shift;
  my $node_name=shift;
  my ($host,$port)=split('_',$node_name);
  my $id=shift;
  my $time=time();
  mylog("Prx:0 Host:${proxysql_host}_${proxysql_port} ACTION:SU $node_name $id");
  $dbh->do("UPDATE mysql_servers SET status='OFFLINE_SOFT', comment='SU:$time' WHERE hostname='$host' AND port='$port' AND hostgroup_id='$id'");
  return 0;
}
sub proxy_del_node ($$$) {
  my $dbh=shift;
  my $node_name=shift;
  my $id=shift;
  my ($host,$port)=split('_',$node_name);
  mylog("Prx:0 Host:${proxysql_host}_${proxysql_port} ACTION:DEL $node_name $id");
  $dbh->do("DELETE FROM mysql_servers WHERE hostname='$host' AND port='$port' AND hostgroup_id='$id'");
  return 0;
}
sub proxy_add_node ($$$) {
  my $dbh=shift;
  my $node_name=shift;
  my $id=shift;
  my $weight=1000;
  my ($host,$port)=split('_',$node_name);
  my $time=time();
  my $comment;
  if ($id == $proxysql_rdid ) { $comment = "RD:$time";  
    mylog("Prx:0 Host:${proxysql_host}_${proxysql_port} ACTION:ADD_RD  $node_name $id"); }
  if ($id == $proxysql_wrid ) { $comment = "WR:$time"; 
    mylog("Prx:0 Host:${proxysql_host}_${proxysql_port} ACTION:ADD_WR  $node_name $id"); }
  if ( !defined $comment ) { $comment = "UN:$time"; 
    mylog("Prx:0 Host:${proxysql_host}_${proxysql_port} ACTION:ADD_UN  $node_name $id"); }
  $dbh->do("INSERT INTO mysql_servers(hostgroup_id,hostname,port,weight,comment) VALUES ('$id','$host','$port','$weight','$comment')");
  return 0;
}
sub thread_proxysql_admin {
  my $database="mysql";
  my $dsn="DBI:mysql:database=$database;host=$proxysql_host;port=$proxysql_port;mysql_connect_timeout=10;mysql_write_timeout=5;mysql_read_timeout=5";
  my %attr = ( PrintError => 0, RaiseError => 0 );
  
 while ( 1 ) {   ### Start main thread loop
  my $dbh;
  until ( $dbh = DBI->connect( $dsn, $proxysql_user, $proxysql_pass, \%attr ) ) {
    mylog("Proxy:0 HOST:$proxysql_host:$proxysql_port Error: Can't connect: $DBI::errstr. Pausing before retrying.");
    sleep $mysql_err_timeout;
  }
  eval {      ### Catch _any_ kind of failures from the code within
    mylog("Prx:0 HOST:$proxysql_host:$proxysql_port OK: Start Execution");
    sleep $mysql_err_timeout;
    $dbh->{RaiseError} = 1;
    $dbh->{AutoCommit} = 1;
    $dbh->{mysql_server_prepare} = 0;

    while (1) { ## Start monitoring thread loop
      my %HASH; # Save statistics by host_port arguments
      my %STAT;
      my @mysql_good_nodes;
      my @mysql_bad_nodes;
      my @proxy_good_write_nodes;
      my @proxy_bad_write_nodes;
      my @proxy_good_read_nodes;
      my @proxy_bad_read_nodes;
      $STAT{'mysql_good_nodes_ref'}=\@mysql_good_nodes;
      $STAT{'mysql_bad_nodes_ref'}=\@mysql_bad_nodes;
      $STAT{'proxy_good_write_nodes_ref'}=\@proxy_good_write_nodes;
      $STAT{'proxy_bad_write_nodes_ref'}=\@proxy_bad_write_nodes;
      $STAT{'proxy_good_read_nodes_ref'}=\@proxy_good_read_nodes;
      $STAT{'proxy_bad_read_nodes_ref'}=\@proxy_bad_read_nodes;
      my $NEED_FLUSH=0;
           
      read_proxy_conf ( $dbh, \%HASH, \%STAT);
      read_mysql_conf ( \%HASH, \%STAT);

      #Start monitoring logic
      #Start check 
      if ( scalar @mysql_good_nodes > 0) { #If we have good nodes status
        for my $tmp_node (@mysql_bad_nodes) {
          if ( $HASH{$tmp_node}{'proxy_read'} == 1 && $HASH{$tmp_node}{'proxy_read_status'} == 0 ) {
            proxy_suspend_node($dbh,$tmp_node,$proxysql_rdid);
            $NEED_FLUSH=1;
          }
          if ( $HASH{$tmp_node}{'proxy_write'} == 1 && $HASH{$tmp_node}{'proxy_write_status'} == 0 ) {
            proxy_suspend_node($dbh,$tmp_node,$proxysql_wrid);
            $NEED_FLUSH=1;
          }
        }
             
        for my $tmp_node (@mysql_good_nodes) {
          if ( $HASH{$tmp_node}{'proxy_read'} == 0 && $HASH{$tmp_node}{'proxy_write'} == 0 ) {
            proxy_add_node($dbh,$tmp_node,$proxysql_rdid); 
            $NEED_FLUSH=1;
          }
        }
        if ( $NEED_FLUSH > 0 ) { read_proxy_conf ( $dbh, \%HASH , \%STAT); }
        if ( scalar @proxy_good_write_nodes == 0 ) { 
        my $tmp = $mysql_good_nodes[rand scalar @mysql_good_nodes];
        proxy_add_node($dbh, $tmp,$proxysql_wrid);
        $NEED_FLUSH=1;
        }
        if ( scalar @proxy_good_write_nodes > 1 )  { 
          my $tmp = rand scalar @mysql_good_nodes;
          for (my $i=0; $i<scalar @mysql_good_nodes; $i++) {
            if ( $i == $tmp ) { continue ; }
            proxy_suspend_node($dbh, $mysql_good_nodes[$i], $proxysql_wrid);
          }
          $NEED_FLUSH=1;
        }
        if ( $NEED_FLUSH > 0 ) { read_proxy_conf ( $dbh, \%HASH , \%STAT); }
        if ( scalar @proxy_good_read_nodes == 0 && scalar @mysql_good_nodes == 1) {
          proxy_add_node($dbh,$mysql_good_nodes[0],$proxysql_rdid);
          $NEED_FLUSH=1;
        }
        if ( scalar @mysql_good_nodes > 1 ) {
          foreach my $tmp_node (@mysql_good_nodes) {
            if ( $HASH{$tmp_node}{'proxy_write'} == 1 && $HASH{$tmp_node}{'proxy_read'} == 1 ) {
              if ($HASH{$tmp_node}{'proxy_write_status'} == 0 && $HASH{$tmp_node}{'proxy_read_status'} == 0 ) {
                proxy_suspend_node($dbh,$tmp_node,$proxysql_rdid);
                $NEED_FLUSH=1;
              }  
            }
          }
        }
        if ( $NEED_FLUSH > 0 ) { read_proxy_conf ( $dbh, \%HASH , \%STAT); }
        my $time=time();
        foreach my $tmp_node (@proxy_bad_write_nodes) {
          if ( ($time-$HASH{$tmp_node}{'proxy_write_change_time'}) > $proxy_suspend_timeout ) {
            proxy_del_node($dbh,$tmp_node,$proxysql_wrid);
            $NEED_FLUSH=1;
          }
        }
        foreach my $tmp_node (@proxy_bad_read_nodes) {
          if ( ($time-$HASH{$tmp_node}{'proxy_read_change_time'}) > $proxy_suspend_timeout ) {
            proxy_del_node($dbh,$tmp_node,$proxysql_rdid);
            $NEED_FLUSH=1;
          }
        }
        if ( $NEED_FLUSH > 0 ) { proxy_flush($dbh); $NEED_FLUSH=0; }
      } 
      else {
        #No any good nodes nothing to do
        mylog("Proxy:0 Host:$proxysql_host:$proxysql_port Err: Zero good nodes. Do nothing")
      }
      sleep $mysql_monitor_timeout;
    }
  }
 }
 mylog("Proxy:0 Host:$proxysql_host:$proxysql_port Err: Monitoring aborted by error: $@") if $@;
 sleep $mysql_err_timeout;
}
exit; ### Never running


  
