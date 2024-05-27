import click

cache = {}

@click.command()
@click.option('--count', default=1, help='Number of greetings.')
@click.option('--name', default='dyman',
              help='The person to greet.')
def hello(count, name):
    """Simple program that greets NAME for a total of COUNT times."""
    for x in range(count):
        click.echo('Hello %s!' % name)

@click.command()
@click.option('--key', default="default", help='cache key')
def get(key):
    click.echo('Get from cache: %s!' % cache[key])

@click.command()
@click.option('--key', default="default", help='cache key')
@click.option('--value', default="default", help='cache value')
def set(key, value):
    cache[key] = value
    click.echo('Set cache:  %s!' % key + "-" + value)

@click.command()
@click.option('--action', default="default", help='cache key')
@click.option('--key', default="default", help='cache key')
@click.option('--value', default="default", help='cache value')
def cache(action, key, value):
    if action == "get":
        click.echo('Get from cache: %s!' % cache[key])
    else:
        cache[key] = value
        click.echo('Set cache:  %s!' % key + "-" + value)

if __name__ == '__main__':
    hello()
