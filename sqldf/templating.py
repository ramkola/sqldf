import os
import textwrap
from datetime import datetime

import typing
from jinja2 import Environment
from jinja2 import FileSystemLoader
from jinja2 import Template
from jinja2 import contextfilter
from jinja2.runtime import Context

JINJA_ENVIRONMENT = Environment()


@contextfilter
def get_timestamp(context: Context, source: datetime) -> str:
    """
    Returns a timestamp string from the source datetime as a context filter
    within templates.

    :param context:
        Jinja2 context in which this filter is being applied
    :param source:
        A datetime object to convert to an SQL datetime query string
    :return:
        The SQL formatted timestamp for the given datetime
    """

    return source.strftime('%Y-%m-%d %H:%M:%S')


def get_environment() -> Environment:
    """
    Returns the jinja2 templating environment updated with the most recent
    environment configurations
    """

    env = JINJA_ENVIRONMENT
    loader = env.loader

    if not loader:
        env.filters['timestamp'] = get_timestamp

    resource_path = os.path.realpath('.')

    if not loader or resource_path not in loader.searchpath:
        env.loader = FileSystemLoader(resource_path)

    return env


def render(template: typing.Union[str, Template], **kwargs) -> str:
    """
    Renders a template string using Jinja2 and the templating environment.

    :param template:
        The string containing the template to be rendered
    :param kwargs:
        Any named arguments to pass to Jinja2 for use in rendering
    :return:
        The rendered template string
    """

    if not hasattr(template, 'render'):
        template = get_environment().from_string(textwrap.dedent(template))

    return template.render(**kwargs)


def render_file(query_file_path: str, **kwargs) -> str:
    """
    Renders a file at the specified absolute path. The file can reside
    anywhere on the local disk as template environment path searching
    is ignored.

    :param query_file_path:
        Absolute path to a template file to render
    :param kwargs:
        Named arguments that should be passed to Jinja2 for rendering
    :return:
        The rendered template string
    """

    with open(query_file_path, 'r') as f:
        contents = f.read()

    return get_environment().from_string(contents).render(**kwargs)
