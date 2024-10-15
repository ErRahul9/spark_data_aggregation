import configparser
import os
import subprocess


def refreshSecurityToken():
    p = subprocess.Popen(['okta-awscli', '--profile', 'integration', '--okta-profile', 'integration'])
    print(p.communicate())

def set_creds():
    refreshSecurityToken()
    config = configparser.ConfigParser()
    config.read(os.path.expanduser('~/.aws/credentials'))
    os.environ['AWS_ACCESS_KEY_ID'] = config['integration']['aws_access_key_id']
    os.environ['AWS_SECRET_ACCESS_KEY'] = config['integration']['aws_secret_access_key']
    os.environ['AWS_SESSION_TOKEN'] = config['integration'].get('aws_session_token', None)

# set_creds()
if __name__ == '__main__':
    set_creds()
    print(os.environ["AWS_ACCESS_KEY_ID"])
    print(f'export {os.environ["AWS_ACCESS_KEY_ID"]}')
    print(f'export {os.environ["AWS_SECRET_ACCESS_KEY"]}')
    print(f'export {os.environ["AWS_SESSION_TOKEN"]}')



