package { 'python-virtualenv':
  ensure => installed,
}
python::virtualenv { '/root/venv':
  ensure       => present,
  requirements => "/root/repo/requirements.txt",
  systempkgs   => true,
  venv_dir     => '/root/venv',
  owner        => 'root',
  group        => 'root',
  cwd          => '/root',
  require      => [
    Package['python-virtualenv'],
  ],
}
