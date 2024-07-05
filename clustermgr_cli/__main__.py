import click
from .cluster_cli import cluster
from .user_cli import user
from .cache_cli import cache
from .tool_cli import tool

@click.group(name='clustermgr')
def clustermgr():
   pass

clustermgr.add_command(cluster)
clustermgr.add_command(cache)
clustermgr.add_command(user)
clustermgr.add_command(tool)

if __name__ == "__main__":
    clustermgr()