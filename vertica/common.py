import paramiko
import argparse
import re

def get_parser(description='TPC-SD Vertica'):
    parser = argparse.ArgumentParser(description=description)
    parser.add_argument('--vertica-username', required=True)
    parser.add_argument('--vertica-password', required=True)
    parser.add_argument('--vertica-database', required=True)
    parser.add_argument('--vertica-host', required=True)
    parser.add_argument('--vertica-identity-file', default=None)
    parser.add_argument('--vertica-port', default='5433')
    return parser

def sanitize_cmd(cmd):
    # remove comments and newlines
    return re.sub('--.*', '', cmd).replace('\n', ' ').strip()

def get_ssh_client(opts):
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    ssh.connect(opts.vertica_host, username='root', key_filename=opts.vertica_identity_file)
    return ssh

def ssh_vertica(ssh_client, command):
    stdin, stdout, stderr = ssh_client.exec_command(command.strip())

    stdout = list(stdout)
    stderr = list(stderr)
    for i, line in enumerate(stdout):
        line = line.rstrip()
        print "%d\t%s" % (i, line)
    for i, line in enumerate(stderr):
        line = line.rstrip()
        print "%d\t%s" % (i, line)
    print ''
    return stdout, stderr

def vsql(ssh_client, opts, sql):
    '''
    Execute some SQL using vsql on the server.
    Be sure to escape any " (double quotes) you use.
    '''

    cleaned_sql = sanitize_cmd(sql)

    return ssh_vertica(ssh_client, '''
        echo "{sql}" | /opt/vertica/bin/vsql -U {vertica_username} -w {vertica_password} -h {vertica_host} -d {vertica_database} -p {vertica_port}
    '''.format(sql=cleaned_sql, **opts.__dict__))

