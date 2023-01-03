"""Ubuntu Layout"""

from ogc.models import Layout

layout = Layout(
    instance_size="e2-standard-4",
    name="ubuntu-ogc",
    provider="google",
    remote_path="/home/ubuntu/ogc",
    runs_on="ubuntu-2004-lts",
    scale=15,
    scripts="fixtures/ogc/elastic-agent-vm/ubuntu",
    username="ubuntu",
    ssh_private_key="fixtures/id_rsa_libcloud",
    ssh_public_key="fixtures/id_rsa_libcloud.pub",
    ports=["22:22", "80:80", "443:443", "5601:5601"],
    tags=[],
    labels=dict(
        division="engineering", org="obs", team="observability", project="perf"
    ),
)
