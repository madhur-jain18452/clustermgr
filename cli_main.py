import click
from clustermgr_cli.cluster_cli import cluster
from clustermgr_cli.user_cli import user
from clustermgr_cli.cache_cli import cache

@click.group(name='clustermgr')
def clustermgr():
    pass


clustermgr.add_command(cluster)
clustermgr.add_command(cache)
clustermgr.add_command(user)


if __name__ == "__main__":
    clustermgr()
