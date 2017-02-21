API_VERSION = '2'

HOST_ADDRESS   = '10.0.2.2'
GUEST_ADDRESS  = '192.168.10.12'
PROXY_ADDRESS  = "http://#{HOST_ADDRESS}:3128/"
GO_PATH        = ENV['GOPATH'] || '.'
WS_PATH        = ENV['WSPATH'] || '.'

Vagrant.configure(API_VERSION) do |config|
  config.vm.box              = 'ubuntu/xenial64'
  config.vm.box_version      = '20161010.0.0'
  config.vm.box_check_update = true
  config.vm.hostname         = 'datapipe-dev'

  config.ssh.forward_agent   = true
  config.vm.network 'private_network', ip: GUEST_ADDRESS

  # Shared directories
  config.vm.synced_folder './',    '/vagrant/', type: 'nfs', mount_options: ['nolock,vers=3,udp,noatime,fsc,actimeo=1']
  config.vm.synced_folder GO_PATH, '/opt/go/',  type: 'nfs', mount_options: ['nolock,vers=3,udp,noatime,fsc,actimeo=1']
  config.vm.synced_folder WS_PATH, '/opt/ws/',  type: 'nfs', mount_options: ['nolock,vers=3,udp,noatime,fsc,actimeo=1']

  # Provision script
  config.vm.provision 'shell' do |s|
    s.path = 'bin/vagrant-provision.sh'
  end

  # Configure the VirtualBox settings for virtual machine that will be created
  config.vm.provider 'virtualbox' do |vb|
    vb.name   = 'go-datapipe'
    vb.memory = 1024
    vb.cpus   = 2
    vb.customize ['modifyvm', :id, '--natdnshostresolver1', 'on']
    vb.customize ['modifyvm', :id, '--natdnsproxy1', 'on']
  end

  if Vagrant.has_plugin?('vagrant-proxyconf')
    config.proxy.http = PROXY_ADDRESS
    config.proxy.https = PROXY_ADDRESS
    config.proxy.no_proxy = ['localhost',
                             '127.0.0.1',
                             GUEST_ADDRESS,
                             HOST_ADDRESS].join(',')
  end
end
