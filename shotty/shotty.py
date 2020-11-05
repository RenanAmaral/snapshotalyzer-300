import boto3
import botocore
import click

def set_profile(profile_name):
    session = boto3.Session(profile_name=profile_name)
    ec2 = session.resource('ec2')

    return ec2

def filter_instances(project, instance):
    instances = []

    if instance:
        filters = [{'Name': 'instance-id', 'Values':[instance]}]
        instances = ec2.instances.filter(Filters=filters)
    elif project:
        filters = [{'Name': 'tag:Project', 'Values':[project]}]
        instances = ec2.instances.filter(Filters=filters)
    else:
        instances = ec2.instances.all()

    return instances

def has_pending_snapshot(volume):
    snapshots = list(volume.snapshots.all())
    return snapshots and snapshots[0].state == 'pending'

@click.group()
@click.option('--profile', 'profile_name', default='shotty', help="Specify a profile")
def cli(profile_name):

    """Shotty manage snapshots"""
    set_profile(profile_name)

@cli.group('snapshots')
def snapshots():
    """Commands for snapshots"""

@snapshots.command('list')
@click.option('--project', default=None,
    help="Only snapshots for project (tag Project:<name>)")
@click.option('--instance', default=None,
    help="Only specified instance")
@click.option('--all', 'list_all', default=False, is_flag=True, help="List all snapshots for each volume, not just most recent")
def list_snapshots(project, list_all, instance):
    "List EC2 snapshots"

    instances = filter_instances(project, instance)

    for i in instances:
        for v in i.volumes.all():
            for s in v.snapshots.all():
                print(", ".join((
                    s.id,
                    v.id,
                    i.id,
                    s.state,
                    s.progress,
                    s.start_time.strftime("%c")
                )))

                if s.state == 'completed' and not list_all: break

    return

@cli.group('volumes')
def volumes():
    """Commands for volumes"""

@volumes.command('list')
@click.option('--project', default=None,
    help="Only instances for project (tag Project:<name>)")
@click.option('--instance', default=None,
    help="Only specified instance")
def list_volumes(project, instance):
    "List EC2 volumes"

    instances = filter_instances(project, instance)

    for i in instances:
        for v in i.volumes.all():
            print(", ".join((
                v.id,
                i.id,
                v.state,
                str(v.size) + "GiB",
                v.encrypted and "Encrypted" or "Not Encrypted"
            )))

    return


@cli.group('instances')
def instances():
    """Command for instances"""

@instances.command('snapshot', help="Create snapshots of all volumes")
@click.option('--project', default=None,
    help="Only volumes for project (tag Project:<name>)")
@click.option('--instance', default=None,
    help="Only specified instance")
@click.option('--force', 'force', default=False, is_flag=True, help="Force snapshots for all instances")
def create_snapshots(project, force, instance):
    "Create snapshots for EC2 instances"

    if not project and not force:
        raise Exception("Choose a project or use --force option to apply for all instances")
        exit()

    instances = filter_instances(project, instance)

    for i in instances:
        last_state = i.state['Name']
        print("Stopping {0}".format(i.id))
        i.stop()
        i.wait_until_stopped()
        for v in i.volumes.all():
            if has_pending_snapshot(v):
                print(" Skipping {0}, snapshot already in progress".format(v.id))
                continue

            print("Creating snapshot of {0}".format(v.id))

            try:
                v.create_snapshot(Description="Created by SnapshotAlyzer 3000")
            except botocore.exceptions.ClientError as e:
                print(" Could not snapshot {0}. ".format(v.id) + str(e))
                continue

        if last_state == 'running':
            print("Starting {0}".format(i.id))
            i.start()
            i.wait_until_running()
        else:
            continue

    print("Job's done!")

    return

@instances.command('list')
@click.option('--project', default=None,
    help="Only instances for project (tag Project:<name>)")
@click.option('--instance', default=None,
    help="Only specified instance")
def list_instances(project, instance):
    "List EC2 instances"

    instances = filter_instances(project, instance)

    for i in instances:
        tags = {t['Key']: t['Value'] for t in i.tags or []}
        print(', '.join((
        i.id,
        i.instance_type,
        i.placement['AvailabilityZone'],
        i.state['Name'],
        i.public_dns_name,
        tags.get('Project', '<no project>'))))
    return

@instances.command('stop')
@click.option('--project', default=None,
    help='Only instances for project')
@click.option('--force', 'force', default=False, is_flag=True, help="Force stop for all instances")
@click.option('--instance', default=None,
    help="Only specified instance")
def stop_instances(project, force, instance):
    "Stop EC2 Instances"

    if not project and not force:
        raise Exception("Choose a project or use --force option to apply for all instances")
        exit()

    instances = filter_instances(project, instance)

    for i in instances:
        print("Stopping {0}..".format(i.id))
        try:
            i.stop()
        except botocore.exceptions.ClientError as e:
            print(" Could not stop {0}. ".format(i.id) + str(e))
            continue

@instances.command('start')
@click.option('--project', default=None,
    help='Only instances for project')
@click.option('--force', 'force', default=False, is_flag=True, help="Force start for all instances")
@click.option('--instance', default=None,
    help="Only specified instance")
def start_instances(project, force, instance):
    "Start EC2 Instances"

    if not project and not force:
        raise Exception("Choose a project or use --force option to apply for all instances")
        exit()

    instances = filter_instances(project, instance)

    for i in instances:
        print("Starting {0}..".format(i.id))
        try:
            i.start()
        except botocore.exceptions.ClientError as e:
            print(" Could not start {0}. ".format(i.id) + str(e))
            continue

@instances.command('reboot')
@click.option('--project', default=None,
    help='Only instances for project')
@click.option('--force', 'force', default=False, is_flag=True, help="Force reboot for all instances")
@click.option('--instance', default=None,
    help="Only specified instance")
def reboot_instances(project, force, instance):
    "Reboot EC2 Instances"

    if not project and not force:
        raise Exception("Choose a project or use --force option to apply for all instances")
        exit()

    instances = filter_instances(project, instance)

    for i in instances:
        print("Rebooting {0}..".format(i.id))
        try:
            i.reboot()
        except botocore.exceptions.ClientError as e:
            print(" Could not reboot {0}. ".format(i.id) + str(e))
            continue

if __name__ == '__main__':

    ec2 = set_profile('shotty')
    cli()
