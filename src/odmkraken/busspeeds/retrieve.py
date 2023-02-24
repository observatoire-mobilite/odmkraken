import typing
import dagster
import paramiko
from pathlib import Path


@dagster.asset(required_resource_keys={'ssh'})
def my_asset(context):
    conn = context.resources.ssh.get_connection()
    with conn.open_sftp() as sftp:
        for file in sftp.listdir_iter('.'):
            sftp.get(file.name, file.name)


class SSHClient:

    def __init__(
        self, host: str, port: int=22, 
        username: typing.Optional[str]=None, 
        password: typing.Optional[str]=None, 
        pkey: typing.Optional[Path]=None,
        hostkey: typing.Optional[str]=None
    ):
        self.transport = paramiko.Transport((host, port))
        self.transport.connect(
            hostkey=hostkey,
            username=username,
            password=password,
            pkey=pkey
        )

    def sftp(self) -> paramiko.SFTPClient:
        return self.transport.open_sftp_client()

    def session(self, **kwargs) -> paramiko.Channel:
        return self.transport.open_session(**kwargs)


@dagster.resource(config_schema={'connection': str})
def db_resource(init_context) -> SSHClient:
    connection = init_context.resource_config["connection"]
    return SSHClient(**connection)