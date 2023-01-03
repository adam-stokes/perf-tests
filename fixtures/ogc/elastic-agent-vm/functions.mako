## Helper template functions downloading/extracting files
<%def name="setup_env()">
if ! test -f "/usr/local/bin/pacapt"; then
    sudo wget -O /usr/local/bin/pacapt https://github.com/icy/pacapt/raw/ng/pacapt
    sudo chmod 755 /usr/local/bin/pacapt
fi
</%def>

<%def name="install_pkgs(pkgs)">
% for pkg in pkgs:
sudo /usr/local/bin/pacapt install --noconfirm ${pkg}
% endfor
</%def>

<%def name="extract(src, dst=None)">
% if dst:
mkdir -p ${dst}
tar -xf ${src} -C ${dst}
% else:
tar -xf ${src}
% endif
</%def>

<%def name="setup_lxd()">
sudo snap install lxd
sudo lxd init --auto
lxc profile set default security.privileged=true
% for net in ["lxdbr0"]:
lxc network set ${net} ipv6.address none || true
# lxc network set ${net} bridge.mtu 1458
% endfor
lxd waitready
</%def>