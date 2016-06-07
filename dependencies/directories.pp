file {'/backups':
  ensure => directory,
  owner  => 'root',
  group  => 'root',
  mode   => '700',
}
file {'/backups/vms':
  ensure => directory,
  owner  => 'root',
  group  => 'root',
  mode   => '700',
}

file {'/etc/rsnapshot/vms':
  ensure => directory,
  owner  => 'root',
  group  => 'root',
  mode   => '700',
}

file { '/var/log/rsnapshot/':
  ensure => directory,
  backup => false,
}
