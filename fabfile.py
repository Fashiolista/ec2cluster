from fabric.api import local, cd
import os
PROJECT_ROOT = os.path.abspath(os.path.join(__file__, '../'))


def publish(test='yes'):
    """ Update pip, create a git tag.
    """
    #if test == 'yes':
    #    validate()

    local('git push')

    from ec2cluster import __version__
    tag_name = 'v%s' % __version__
    local('python setup.py sdist upload')

    local('git tag %s' % tag_name)
    local('git push origin --tags')


def validate():
    with cd(PROJECT_ROOT):
        local('pep8 --exclude=migrations --ignore=E501,E225 ec2cluster')
        local('pyflakes.py -x W ec2cluster')
        local('python -m unittest ec2cluster.tests')


def clean():
    local('bash -c "autopep8 -i *.py"')
    local('bash -c "autopep8 -i ec2cluster/*.py"')
    local('bash -c "autopep8 -i ec2cluster/utils/*.py"')
