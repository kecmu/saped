import csv
import paramiko
import numpy as np
from scp import SCPClient
import time

__author__ = 'Ke Wang'
__copyright__ = 'Copyright 2017, Ke Wang'
__version__ = '0.5'
__email__ = 'kewang1@andrew.cmu.edu'


server = {
    'ip': "10.1.1.51",
    'user': 'root',
    'pwd': 'theOne!penguinsFTW'
}

db_server = {
    'ip': "10.1.1.49",
    'user': 'root',
    'pwd': 'theOne!penguinsFTW'
}

client = {
    'ip': "10.1.1.9",
    'user': 'root',
    'pwd': 'theOne!penguinsFTW'
}

vm_credential = {
    'user': 'root',
    'pwd': 'fedora'
}


def generate_traffic():
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    ssh.connect("10.1.100.5", username=vm_credential['user'], password=vm_credential['pwd'])
    print("Start workload")
    stdin, stdout, stderr = ssh.exec_command('/home/ke/generator.sh')
    print("Left workload generating")
    output_file = 'test'
    with open(output_file, 'w') as file:
        for line in stdout.readlines():
            file.write("%s" % line)
    ssh.close()


def network_monitor(duration=20, reso=1, output_file='packet.csv'):
    ssh_server = paramiko.SSHClient()
    ssh_server.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    ssh_server.connect(server['ip'], username=server['user'], password=server['pwd'])
    print("Start network monitoring")

    duration *= 1
    stdin, stdout, stderr = ssh_server.exec_command('sudo -S /home/ke/sniffer.sh %s %s' % (int(duration/reso), reso))
    stdin.write('theOne!penguinsFTW\n')
    stdin.flush()
    print("Left network monitoring")

    print("Capture err: %s" % stderr.readlines())
    with open(output_file, 'w') as network_file:
        for line in stdout.readlines():
            network_file.write("%s" % line)

    ssh_server.close()


def cpu_insert(duration=20, bound=10, sleep_dur=30):
    time.sleep(sleep_dur)
    ssh_server = paramiko.SSHClient()
    ssh_server.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    ssh_server.connect(server['ip'], username=server['user'], password=server['pwd'])
    print("Start CPU consumption")

    stdin, stdout, stderr = ssh_server.exec_command('sudo -S timeout %s /home/ke/cpucontrol.sh %s' % (duration, bound))
    stdin.write('theOne!penguinsFTW\n')
    stdin.flush()
    print("Capture err: %s" % stderr.readlines())
    ssh_server.close()


def cpu_vm(duration=20, sleep_dur=30):
    time.sleep(sleep_dur)
    ssh_vm = paramiko.SSHClient()
    ssh_vm.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    ssh_vm.connect("10.1.100.6", username=vm_credential['user'], password=vm_credential['pwd'])
    print("Start CPU Interference")

    stdin, stdout, stderr = ssh_vm.exec_command('sudo -S stress -c 8 -t %s' % duration)
    stdin.write('theOne!penguinsFTW\n')
    stdin.flush()
    print("Capture err: %s" % stderr.readlines())
    ssh_vm.close()


def cpu_resize(duration=20, sleep_dur=30):
    time.sleep(sleep_dur)
    ssh_vm = paramiko.SSHClient()
    ssh_vm.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    ssh_vm.connect("10.1.100.4", username=vm_credential['user'], password=vm_credential['pwd'])
    print("Start CPU Interference")

    stdin, stdout, stderr = ssh_vm.exec_command('sudo -S echo 0 > /sys/devices/system/cpu/cpu3/online')
    stdin.write('theOne!penguinsFTW\n')
    stdin.flush()
    stdin, stdout, stderr = ssh_vm.exec_command('sudo -S echo 0 > /sys/devices/system/cpu/cpu2/online')
    stdin.write('theOne!penguinsFTW\n')
    stdin.flush()
    stdin, stdout, stderr = ssh_vm.exec_command('sudo -S echo 0 > /sys/devices/system/cpu/cpu1/online')
    stdin.write('theOne!penguinsFTW\n')
    stdin.flush()

    time.sleep(duration)
    stdin, stdout, stderr = ssh_vm.exec_command('sudo -S echo 1 > /sys/devices/system/cpu/cpu3/online')
    stdin.write('theOne!penguinsFTW\n')
    stdin.flush()
    stdin, stdout, stderr = ssh_vm.exec_command('sudo -S echo 1 > /sys/devices/system/cpu/cpu2/online')
    stdin.write('theOne!penguinsFTW\n')
    stdin.flush()
    stdin, stdout, stderr = ssh_vm.exec_command('sudo -S echo 1 > /sys/devices/system/cpu/cpu1/online')
    stdin.write('theOne!penguinsFTW\n')
    stdin.flush()
    print("Capture err: %s" % stderr.readlines())
    ssh_vm.close()


def network_insert(duration=20, sleep_dur=30, src_ip="10.1.1.9"):
    time.sleep(sleep_dur)
    ssh_vm = paramiko.SSHClient()
    ssh_vm.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    ssh_vm.connect(src_ip, username=server['user'], password=server['pwd'])

    stdin, stdout, stderr = ssh_vm.exec_command('sudo -S timeout %s hping3 --flood 10.1.100.5 -d 1000 -q' % duration)
    stdin.write('theOne!penguinsFTW\n')
    stdin.flush()
    print("Capture err: %s" % stderr.readlines())
    ssh_vm.close()


def lossy_network(duration=20, sleep_dur=30):
    print("%s, %s" % (sleep_dur, duration))
    time.sleep(sleep_dur)
    ssh_vm = paramiko.SSHClient()
    ssh_vm.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    ssh_vm.connect("10.1.100.5", username=vm_credential['user'], password=vm_credential['pwd'])

    stdin, stdout, stderr = ssh_vm.exec_command(
        'sudo -S iptables -I INPUT 1 -m statistic --mode random --probability 0.02 -j DROP')
    stdin.write('theOne!penguinsFTW\n')
    stdin.flush()

    time.sleep(duration)

    stdin, stdout, stderr = ssh_vm.exec_command(
        'sudo -S iptables -D INPUT -m statistic --mode random --probability 0.02 -j DROP')
    stdin.write('theOne!penguinsFTW\n')
    stdin.flush()
    print("Capture err: %s" % stderr.readlines())
    ssh_vm.close()


def process_network_data(topic, index):
    network_filename = "%s_network_exp_%s" % (topic, index)
    with open(network_filename) as f:
        packet_content = f.readlines()
    in_count = []
    out_count = []
    in_bad = []
    out_bad = []
    measure_time = []
    for i in packet_content:
        ele = i.split(",")
        if len(ele) != 6:
            continue
        in_can = int(ele[1]) + int(ele[2]) - int(ele[3])
        measure_time.append(int(ele[0]))
        in_count.append(int(ele[1]))
        in_bad.append(int(ele[2]))
        out_bad.append(int(ele[3]))
        out_count.append(int(ele[4]))

    in_diff_count = [in_count[0]]
    out_diff_count = [out_count[0]]
    sum_in = in_count[-1]
    sum_out = out_count[-1]

    for i in range(1, len(in_count)):
        in_diff_count.append((in_count[i]-in_count[i-1]) + (in_bad[i]-in_bad[i-1]) - (out_bad[i]-out_bad[i-1]))
        out_diff_count.append((out_count[i]-out_count[i-1]))
        #if in_diff_count[-1] > 100:
        #    in_diff_count[-1] = in_diff_count[-2]
        #    out_diff_count[-1] = out_diff_count[-2]

    return measure_time, in_diff_count, out_diff_count, sum_in, sum_out


def confidence(a, b, start, win):
    # given two vectors a and b, compute the maximal cross correlation coefficient between a[0:win] and various
    # b slices: b[0:win], b[1:win+1],... b[-win:]
    if sum(b[start:start + win]) == 0:
        return 0
    tmp = np.corrcoef(a[start:start+win], b[start:start+win])
    coefficient = tmp[1][0]
    return coefficient


def corr(a, b):
    if np.var(a)*np.var(b) == 0:
        return 0

    coefficient = np.corrcoef(a, b)
    return coefficient[1][0]
