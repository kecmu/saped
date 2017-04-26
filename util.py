import csv
import paramiko
import numpy as np
from scp import SCPClient
import time
import math

__author__ = 'Ke Wang'
__copyright__ = 'Copyright 2017, Ke Wang'
__version__ = '0.3'
__email__ = 'kewang1@andrew.cmu.edu'


server = {
    'ip': "10.1.1.49",
    'user': 'root',
    'pwd': 'theOne!penguinsFTW'
}

db_server = {
    'ip': "10.1.128.37",
    'user': 'ubuntu',
    'pwd': 'ubuntu'
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


def generate_traffic(output_file='clientRT.csv', load=0.1, dur=10, mon_dur=20, init_load=0.04, resolution=1):
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    ssh.connect(client['ip'], username=client['user'], password=client['pwd'])
    print("Start workload")
    # stdin, stdout, stderr = ssh.exec_command('python3 /home/ke/generator_deadlock.py --load=%s '
    #                                          '--duration=%s --mon_dur=%s --start_load=%s '
    #                                          '--resolution=%s --time_inc=60 --time_stop=80' %
    #                                          (load, dur, mon_dur, init_load, resolution))

    stdin, stdout, stderr = ssh.exec_command('python3 /home/ke/generator_hv.py --load=%s '
                                             '--duration=%s --mon_dur=%s --start_load=%s '
                                             '--resolution=%s' %
                                             (load, dur, mon_dur, init_load, resolution))

    print("Left workload generating")

    with open(output_file, 'w') as csvfile:
        w = csv.writer(csvfile)
        w.writerow(['Time', 'Response Time'])
        for line in stdout.readlines():
            w.writerow([f.strip() for f in line.split(',')])
    scp = SCPClient(ssh.get_transport())
    qfile = output_file+"_queue_file"
    outfile = output_file+"_output_file"
    scp.get('/root/qfile.txt', qfile)
    scp.get('/root/output.txt', outfile)

    ssh.close()


def tcp_monitor(duration=20, reso=1, output_file='tcpqueue.csv'):
    ssh_server = paramiko.SSHClient()
    ssh_server.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    ssh_server.connect(server['ip'], username=server['user'], password=server['pwd'])
    print("Start tcp monitoring")

    stdin, stdout, stderr = ssh_server.exec_command('bash ./tcpqueue.sh %s %s'
                                                    % (int(duration/reso), reso))
    print("Left tcp monitoring")

    print("Capture err: %s" % stderr.readlines())
    with open(output_file, 'w') as tcpfile:
        for line in stdout.readlines():
            tcpfile.write("%s" % line)

    ssh_server.close()


def sar_monitor(duration=60, output_file='page.csv'):
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    # ssh.connect(server['ip'], username=server['user'], password=server['pwd'])
    ssh.connect("10.1.100.1", username=vm_credential['user'], password=vm_credential['pwd'])
    print("start page statistics monitoring")
    monitor_count = math.ceil(duration/2)
    stdin, stdout, stderr = ssh.exec_command('sar -urwBqW 2 %s' % monitor_count)

    print("Left workload generating")

    with open(output_file, 'w') as file:
        for line in stdout.readlines():
            file.write("%s" % line)

    ssh.close()


def network_monitor(duration=20, reso=1, output_file='packet.csv'):
    ssh_server = paramiko.SSHClient()
    ssh_server.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    ssh_server.connect(server['ip'], username=server['user'], password=server['pwd'])
    print("Start network monitoring")

    # reso = 1
    duration *= 1

    stdin, stdout, stderr = ssh_server.exec_command('sudo -S /home/ke/sniffer.sh %s %s'
                                                    % (int(duration/reso), reso))
    stdin.write('theOne!penguinsFTW\n')
    stdin.flush()
    print("Left network monitoring")

    print("Network monitor err: %s" % stderr.readlines())
    with open(output_file, 'w') as network_file:
        for line in stdout.readlines():
            network_file.write("%s" % line)

    ssh_server.close()


def burst_insert(load=0.1, duration=30, src="10.1.1.42", sleep_dur=30):
    time.sleep(sleep_dur)
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    ssh.connect(src, username=server['user'], password=server['pwd'])
    print("Start workload injection")
    stdin, stdout, stderr = ssh.exec_command('python3 /home/ke/client_insert.py --load=%s '
                                             '--duration=%s --mon_dur=%s --start_load=%s '
                                             '--resolution=0.2' % (load, duration, duration, load))

    print("burst workload injection err: %s" % stderr.readlines())
    ssh.close()


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


def cpu_vm(duration=20, sleep_dur=30, src_ip="10.1.100.3"):
    time.sleep(sleep_dur)
    ssh_vm = paramiko.SSHClient()
    ssh_vm.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    ssh_vm.connect(src_ip, username=vm_credential['user'], password=vm_credential['pwd'])
    print("Start CPU Interference")

    stdin, stdout, stderr = ssh_vm.exec_command('sudo -S stress -c 4 -t %s' % duration)
    stdin.write('theOne!penguinsFTW\n')
    stdin.flush()
    print("Capture err: %s" % stderr.readlines())
    ssh_vm.close()


def cpu_hog(duration=20, sleep_dur=30, src_ip="10.1.100.1"):
    time.sleep(sleep_dur)
    ssh_vm = paramiko.SSHClient()
    ssh_vm.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    ssh_vm.connect(src_ip, username=vm_credential['user'], password=vm_credential['pwd'])
    print("Start CPU hog")

    # stdin, stdout, stderr = ssh_vm.exec_command('sudo -S timeout %s /home/ke/cpu_hog' % duration)
    stdin, stdout, stderr = ssh_vm.exec_command('sudo -S stress -c 3 -t %s' % duration)
    stdin.write('theOne!penguinsFTW\n')
    stdin.flush()
    print("CPU hog injection err: %s" % stderr.readlines())
    ssh_vm.close()


def mem_hog(duration=20, sleep_dur=30, src_ip="10.1.100.1"):
    time.sleep(sleep_dur)
    ssh_vm = paramiko.SSHClient()
    ssh_vm.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    ssh_vm.connect(src_ip, username=vm_credential['user'], password=vm_credential['pwd'])
    print("Start memory hog")

    stdin, stdout, stderr = ssh_vm.exec_command('sudo -S timeout %s /home/ke/mem_stress' % duration)

    stdin.write('theOne!penguinsFTW\n')
    stdin.flush()
    print("Memory hog injection err: %s" % stderr.readlines())
    ssh_vm.close()


def io_hog(duration=20, sleep_dur=30, src_ip="10.1.100.1"):
    time.sleep(sleep_dur)
    ssh_vm = paramiko.SSHClient()
    ssh_vm.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    ssh_vm.connect(src_ip, username=vm_credential['user'], password=vm_credential['pwd'])
    print("Start IO hog")

    stdin, stdout, stderr = ssh_vm.exec_command('sudo -S timeout %s /home/ke/mem_stress' % duration)

    stdin.write('theOne!penguinsFTW\n')
    stdin.flush()
    print("IO hog injection err: %s" % stderr.readlines())
    ssh_vm.close()


def cpu_resize(duration=20, sleep_dur=30, src_ip="10.1.100.1"):
    time.sleep(sleep_dur)
    ssh_vm = paramiko.SSHClient()
    ssh_vm.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    ssh_vm.connect(src_ip, username=vm_credential['user'], password=vm_credential['pwd'])
    print("Start CPU resize")

    stdin, stdout, stderr = ssh_vm.exec_command('sudo -S echo 0 > /sys/devices/system/cpu/cpu2/online')
    stdin.write('theOne!penguinsFTW\n')
    stdin.flush()
    stdin, stdout, stderr = ssh_vm.exec_command('sudo -S echo 0 > /sys/devices/system/cpu/cpu1/online')
    stdin.write('theOne!penguinsFTW\n')
    stdin.flush()

    time.sleep(duration)
    stdin, stdout, stderr = ssh_vm.exec_command('sudo -S echo 1 > /sys/devices/system/cpu/cpu2/online')
    stdin.write('theOne!penguinsFTW\n')
    stdin.flush()
    stdin, stdout, stderr = ssh_vm.exec_command('sudo -S echo 1 > /sys/devices/system/cpu/cpu1/online')
    stdin.write('theOne!penguinsFTW\n')
    stdin.flush()
    print("CPU resize err: %s" % stderr.readlines())
    ssh_vm.close()


def network_insert(duration=20, sleep_dur=30, src_ip="10.1.1.42"):
    time.sleep(sleep_dur)
    ssh_vm = paramiko.SSHClient()
    ssh_vm.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    ssh_vm.connect(src_ip, username=server['user'], password=server['pwd'])
    print("start injecting network congestion error")

    stdin, stdout, stderr = ssh_vm.exec_command('sudo -S timeout %s hping3 --flood 10.1.1.9 -q' % duration)
    stdin.write('theOne!penguinsFTW\n')
    stdin.flush()
    print("Injection err: %s" % stderr.readlines())
    ssh_vm.close()


def net_hog(duration=20, sleep_dur=30, src_ip="10.1.1.42"):
    time.sleep(sleep_dur)
    ssh_vm = paramiko.SSHClient()
    ssh_vm.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    ssh_vm.connect(src_ip, username=server['user'], password=server['pwd'])
    print("start injecting network hog error")

    stdin, stdout, stderr = ssh_vm.exec_command('sudo -S timeout %s hping3 -i u10 10.1.100.1 -d 1000 -q' % duration)
    stdin.write('theOne!penguinsFTW\n')
    stdin.flush()
    print("Injection err: %s" % stderr.readlines())
    ssh_vm.close()


def lossy_network(duration=20, sleep_dur=30):
    time.sleep(sleep_dur)
    ssh_vm = paramiko.SSHClient()
    ssh_vm.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    ssh_vm.connect(client['ip'], username=client['user'], password=client['pwd'])
    print("start injecting loss network anomaly")

    stdin, stdout, stderr = ssh_vm.exec_command(
        'sudo -S /home/ke/loss_network.sh %s' % duration)
    stdin.write('theOne!penguinsFTW\n')
    stdin.flush()

    print("Loss network injection err: %s" % stderr.readlines())
    ssh_vm.close()


def process_tcp_data(topic, traffic_load, index, dur):
    tcp_filename = "%s_tcp_load_%s_exp_%s_dur_%s" % (topic, traffic_load, index, dur)
    with open(tcp_filename) as f:
        tcpcontent = f.readlines()
    tcp_content = []
    tcp_time = []
    for i in tcpcontent:
        ele = i.split(",")
        tcp_content.append(float(ele[1]))
        tcp_time.append(float(ele[0]))

    return tcp_time, tcp_content


def process_network_data(topic, traffic_load, index, dur):
    network_filename = "%s_network_load_%s_exp_%s_dur_%s" % (topic, traffic_load, index, dur)
    with open(network_filename) as f:
        packet_content = f.readlines()
    in_count = []
    out_count = []
    measure_time = []
    for i in packet_content:
        ele = i.split(",")
        if len(ele) != 4:
            continue
        measure_time.append(int(ele[0]))
        in_count.append(int(ele[1]))
        out_count.append(int(ele[2]))

    in_diff_count = [in_count[0]]
    out_diff_count = [out_count[0]]
    sum_in = in_count[-1]
    sum_out = out_count[-1]

    for i in range(1, len(in_count)):
        in_diff_count.append(in_count[i] - in_count[i - 1])
        out_diff_count.append(out_count[i] - out_count[i - 1])

    return measure_time, in_diff_count, out_diff_count, sum_in, sum_out


def process_network_data2(topic, traffic_load, index, dur):
    network_filename = "%s_network_load_%s_exp_%s_dur_%s" % (topic, traffic_load, index, dur)
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


def process_client_queue_data(topic, traffic_load, index, dur, outputfile, outcountfile, start, resolution):
    with open(outputfile) as f:
        qcontent = f.readlines()

    q_content = []
    q_timeline = []
    for qlen in qcontent:
        raw = qlen.strip('[')
        tmp1 = raw.strip('\n')
        tmp2 = tmp1.strip(']')
        ele = tmp2.split(", ")
        if len(ele) != 2:
            continue
        q_content.append(float(ele[1]))
        q_timeline.append(float(ele[0]))

    with open(outcountfile) as f:
        out_count = f.readlines()

    out_content = []
    out_samples = []
    for out_c in out_count:
        raw = out_c.strip('[')
        tmp1 = raw.strip('\n')
        tmp2 = tmp1.strip(']')
        ele = tmp2.split(", ")
        if len(ele) != 2:
            continue
        out_content.append(float(ele[1]))

    out_diff = [0]
    out_diff.extend(np.diff(out_content))
    out_content = []
    out_indexes = []
    for i in range(len(q_timeline)):
        out_index = int((int(q_timeline[i]) - start) / resolution)
        cur_len = len(out_content)
        if out_index >= cur_len:
            out_content.extend([0 for _ in range(out_index + 1 - cur_len)])
            out_samples.extend([0 for _ in range(out_index + 1 - cur_len)])
            out_indexes.extend([0 for _ in range(out_index + 1 - cur_len)])
            out_indexes[-1] = out_index
        pre_count = out_samples[out_index]
        out_content[out_index] = (pre_count/(pre_count+1))*out_content[out_index]+(1/(pre_count+1))*int(out_diff[i])
        out_samples[out_index] += 1

    return q_timeline, q_content, out_indexes, out_content


def process_client_rt_data(topic, traffic_load, index, dur, resolution):
    delay_file_name = "%s_clientRT_load_%s_exp_%s_dur_%s" % (topic, traffic_load, index, dur)
    resolution_in_milli = resolution * 1000

    rt_time = [0]
    rt_count = [0]
    rt_indexes = [0]

    with open(delay_file_name) as delay_file:
        rtcontent = delay_file.readlines()

    rtcontent.pop(0)
    rt_start = int(rtcontent.pop(0).split(',')[0])
    client_start = 0

    for line in rtcontent:
        if line == '\n':
            continue
        ele = line.split(',')
        if len(ele) > 2 or len(ele) == 0:
            continue
        if client_start == 0:
            if int(ele[1]) > 0:
                client_start = int(ele[0])

        rt_index = int((int(ele[0]) - rt_start) / resolution_in_milli)
        cur_len = len(rt_time)
        if rt_index >= cur_len:
            rt_count.extend([0 for _ in range(rt_index + 1 - cur_len)])
            rt_time.extend([0 for _ in range(rt_index + 1 - cur_len)])
            rt_indexes.extend([0 for _ in range(rt_index + 1 - cur_len)])
        pre_count = rt_count[rt_index]
        rt_time[rt_index] = (pre_count / (pre_count + 1)) * rt_time[rt_index] + (1 / (pre_count + 1)) * int(ele[1])
        rt_count[rt_index] += 1

    return rt_start, rt_time, rt_count, rt_indexes, client_start


def process_sar_data(filename):
    with open(filename) as f:
        content = f.readlines()
    samples = int((len(content)-1)/18 - 1)

    sample_start = get_sec(content[2].split()[0])  # the system time in seconds that monitoring begins
    cpu_user_raw = []
    cpu_sys_raw = []
    mem_used_raw = []
    ctxt_raw = []
    page_in_raw = []
    page_out_raw = []
    page_fault_raw = []
    swap_in_raw = []
    swap_out_raw = []
    queue_raw = []

    for i in range(samples):
        cpu_line = content[3+i*18].split()
        cpu_user_raw.append(float(cpu_line[3]))
        cpu_sys_raw.append(float(cpu_line[5]))
        mem_used_raw.append(float(content[15+i*18].split()[4]))
        ctxt_raw.append(float(content[6+i*18].split()[3]))
        page_line = content[12+i*18].split()
        page_in_raw.append(float(page_line[2]))
        page_out_raw.append(float(page_line[3]))
        page_fault_raw.append(float(page_line[4]))
        swap_in_raw.append(float(content[9+i*18].split()[2]))
        swap_out_raw.append(float(content[9+i*18].split()[3]))
        queue_raw.append(float(content[18+i*18].split()[2]))

    # smooth the raw data using 10-sample moving average
    cpu_user = list(cpu_user_raw)
    cpu_sys = list(cpu_sys_raw)
    mem_used = list(mem_used_raw)
    ctxt = list(ctxt_raw)
    page_in = list(page_in_raw)
    page_out = list(page_out_raw)
    page_fault = list(page_fault_raw)
    swap_in = list(swap_in_raw)
    swap_out = list(swap_out_raw)
    queue = list(queue_raw)
    for i in range(9, len(cpu_user_raw)):
        cpu_user[i] = np.mean(cpu_user_raw[i-9:i+1])
        cpu_sys[i] = np.mean(cpu_sys_raw[i-9:i+1])
        mem_used[i] = np.mean(mem_used_raw[i-9:i+1])
        ctxt[i] = np.mean(ctxt_raw[i-9:i+1])
        page_in[i] = np.mean(page_in_raw[i-9:i+1])
        page_out[i] = np.mean(page_out_raw[i-9:i+1])
        page_fault[i] = np.mean(page_fault_raw[i-9:i+1])
        swap_in[i] = np.mean(swap_in_raw[i-9:i+1])
        swap_out[i] = np.mean(swap_out_raw[i-9:i+1])
        queue[i] = np.mean(queue_raw[i-9:i+1])
    for i in range(0):
        cpu_user[i] = cpu_user[9]
        cpu_sys[i] = cpu_sys[9]
        mem_used[i] = mem_used[9]
        ctxt[i] = ctxt[9]
        page_in[i] = page_in[9]
        page_out[i] = page_out[9]
        page_fault[i] = page_fault[9]
        swap_in[i] = swap_in[9]
        swap_out[i] = swap_out[9]
        queue[i] = queue[9]

    return sample_start, cpu_user, cpu_sys, mem_used, ctxt, page_in, page_out, page_fault, swap_in, swap_out, queue


def get_sec(time_str):
    h, m, s = time_str.split(':')
    return int(h) * 3600 + int(m) * 60 + int(s)


def distance(instance1, instance2):
    dist = 0
    for x in range(len(instance1)):
        dist += abs(instance1[x] - instance2[x])
    return dist


def data_point_gen(list_raw):
    num_data = int(len(list_raw)/10)
    list_gen = []
    for i in range(num_data):
        list_gen.append(list_raw[i*10:(i+1)*10])
    return list_gen


# return the average distance to the k-nearest neighbors
def dist2neighbors(normal_set, data_point, k):
    distances = []
    for i in range(len(normal_set)):
        distances.append(distance(data_point, normal_set[i]))
    sort_dist = sorted(distances)
    return np.mean(sort_dist[1:k+1])


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


def detection_find(sample_time, correlation, search_start, corr_thr):
    for i in range(len(sample_time)):
        if sample_time[i] < search_start:
            continue
        else:
            if correlation[i] < corr_thr:
                return sample_time[i]
    return -1


def anomaly_find(rt_indexes, search_start, rt):
    for i in range(len(rt_indexes)):
        if rt_indexes[i] < search_start:
            continue
        else:
            if rt[i] > 1000:
                return rt_indexes[i]
    return -1


def lag_find(sample_time, rt_indexes, correlation, rt, corr_thr, search_start):
    a = anomaly_find(rt_indexes, search_start, rt)
    b = detection_find(sample_time, correlation, search_start, corr_thr)
    if (a+50) > b > a:
        return b-a
    elif a+20 > b+20 > a:
        return 0
    else:
        print("b: %s, a: %s" % (b, a))
        return -1

