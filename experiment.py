from threading import Thread
import matplotlib.pyplot as plt
from util import *
import time

__author__ = 'Ke Wang'
__copyright__ = 'Copyright 2016, Ke Wang'
__version__ = '0.3'
__email__ = 'kewang1@andrew.cmu.edu'


def experiment(e_load, monitor_len, e_dur, resolution=1.0, v_resolution=0.2, e_index=0, e_topic='', col=False, win=10,
               ini=0.04, efilter=False, inject_dur=20, interf_time=[], burst_time=[], resize_time=[], net_time=[],
               net_hog_time=[], cpu_hog_time=[], mem_hog_time=[], io_hog_time=[]):
    output_file = "%s_clientRT_load_%s_exp_%s_dur_%s" % (e_topic, e_load, e_index, e_dur)
    if col:
        # need to collect raw data
        # launch workload generator
        print("Start workload generator thread")
        t_generator = Thread(target=generate_traffic, kwargs={'load': e_load, 'dur': e_dur,
                                                              'mon_dur': int(monitor_len/resolution), 'init_load': ini,
                                                              'resolution': resolution, 'output_file': output_file})
        print("Start network_monitor thread")
        t_network_monitor = Thread(target=network_monitor, kwargs={'duration': monitor_len, 'reso': resolution,
                                                                   'output_file': '%s_network_load_%s_exp_%s_dur_%s'
                                                                                  % (e_topic, e_load, e_index, e_dur)})

        t_sar_monitor = Thread(target=sar_monitor, kwargs={'duration': monitor_len,
                                                           'output_file': '%s_page_load_%s_exp_%s_dur_%s'
                                                                          % (e_topic, e_load, e_index, e_dur)})
        t_generator.start()
        t_network_monitor.start()
        t_sar_monitor.start()
        print("Experimenting...")
        # t_cpu_insert = Thread(target=cpu_insert, kwargs={'duration': cpu_dur, 'bound': cpu_bound,
        #                                                  'sleep_dur': cpu_sleep_dur})

        # threads to inject VM interference anomaly
        cpu_interference_threads = []
        for i in range(len(interf_time)):
            cpu_interference_threads.append(Thread(target=cpu_vm, kwargs={'duration': inject_dur,
                                                                          'sleep_dur': interf_time[i],
                                                                          'src_ip': "10.1.100.3"}))
        # threads to inject VM resize anomaly
        resize_threads = []
        for i in range(len(resize_time)):
            resize_threads.append(Thread(target=cpu_resize, kwargs={'duration': inject_dur, 'sleep_dur': resize_time[i],
                                                                    'src_ip': "10.1.100.1"}))
        # threads to inject bursty workload anomaly
        burst_threads = []
        for i in range(len(burst_time)):
            burst_threads.append(Thread(target=burst_insert, kwargs={'duration': inject_dur, 'sleep_dur': burst_time[i],
                                                                     'src': "10.1.1.42", 'load': 0.07}))
        # threads to inject lossy network anomaly
        loss_net_threads = []
        for i in range(len(net_time)):
            loss_net_threads.append(Thread(target=lossy_network, kwargs={'duration': inject_dur,
                                                                         'sleep_dur': net_time[i]}))
        # threads to inject network-hog anomaly
        net_hog_threads = []
        for i in range(len(net_hog_time)):
            net_hog_threads.append(Thread(target=net_hog, kwargs={'duration': inject_dur,
                                                                  'sleep_dur': net_hog_time[i]}))
        # threads to inject cpu-hog anomaly
        cpu_hog_threads = []
        for i in range(len(cpu_hog_time)):
            cpu_hog_threads.append(Thread(target=cpu_hog, kwargs={'duration': inject_dur,
                                                                  'sleep_dur': cpu_hog_time[i]}))

        # threads to inject io-hog anomaly
        mem_hog_threads = []
        for i in range(len(mem_hog_time)):
            mem_hog_threads.append(Thread(target=mem_hog, kwargs={'duration': inject_dur,
                                                                  'sleep_dur': mem_hog_time[i]}))

        for i in range(len(interf_time)):
            cpu_interference_threads[i].start()
        for i in range(len(resize_time)):
            resize_threads[i].start()
        for i in range(len(burst_time)):
            burst_threads[i].start()
        for i in range(len(net_time)):
            loss_net_threads[i].start()
        for i in range(len(net_hog_time)):
            net_hog_threads[i].start()
        for i in range(len(cpu_hog_time)):
            cpu_hog_threads[i].start()
        for i in range(len(mem_hog_time)):
            mem_hog_threads[i].start()

        for i in range(len(interf_time)):
            cpu_interference_threads[i].join()
        for i in range(len(resize_time)):
            resize_threads[i].join()
        for i in range(len(burst_time)):
            burst_threads[i].join()
        for i in range(len(net_time)):
            loss_net_threads[i].join()
        for i in range(len(net_hog_time)):
            net_hog_threads[i].join()
        for i in range(len(cpu_hog_time)):
            cpu_hog_threads[i].join()
        for i in range(len(mem_hog_time)):
            mem_hog_threads[i].join()

        t_network_monitor.join()
        t_generator.join()
        t_sar_monitor.join()
        print("Finishing gathering data, begin processing...")

    resolution_in_milli = v_resolution * 1000
    network_time, in_diff_count, out_diff_count, sum_in, sum_out = process_network_data(
        topic=e_topic, traffic_load=e_load, index=e_index, dur=e_dur)
    rt_start, rt_time, rt_count, rt_indexes, client_start = \
        process_client_rt_data(topic=e_topic, traffic_load=e_load, index=e_index, dur=e_dur, resolution=v_resolution)

    q_time, q_content, out_index, out_content = process_client_queue_data(topic=e_topic, traffic_load=e_load,
                                                                          index=e_index, dur=e_dur,
                                                                          outputfile=output_file + "_queue_file",
                                                                          outcountfile=output_file + "_output_file",
                                                                          start=rt_start,
                                                                          resolution=resolution_in_milli)

    queue_client = []
    qc_count = []
    for i in range(len(q_content)):
        index_raw = (int(q_time[i]) - rt_start) / resolution_in_milli
        q_index = int(index_raw)
        cur_len = len(queue_client)
        if q_index >= cur_len:
            queue_client.extend([0 for _ in range(q_index + 1 - cur_len)])
            qc_count.extend([0 for _ in range(q_index + 1 - cur_len)])
        queue_client[q_index] = (1/(qc_count[q_index]+1))*q_content[i] +\
                                (qc_count[q_index]/(qc_count[q_index]+1))*queue_client[q_index]
        qc_count[q_index] += 1

    # calculate average real response time
    sum_rt = 0
    sum_count = 0
    for i in range(len(rt_time)):
        sum_rt += rt_time[i]*rt_count[i]
        sum_count += rt_count[i]
    mean_rt = sum_rt/sum_count
    print("mean rt: %s" % mean_rt)

    # Adjust the time difference between server machine and client machine.
    # We measure the time for the same event: the arrival of the first request.
    server_start = 0
    for i in range(len(in_diff_count)):
        if server_start == 0:
            if in_diff_count[i] > 0:
                server_start = network_time[i]

    # print("%s, %s " % (server_start, client_start))
    c_s_diff = server_start - client_start

    # Adjust server network measurement to a unified time line
    for i in range(len(network_time)):
        network_time[i] -= c_s_diff

    sample_time = list(network_time)
    for i in range(len(sample_time)):
        sample_time[i] = network_time[i] - rt_start
        if sample_time[i] < 0:
            sample_time[i] = 0
        sample_time[i] /= 1000

    arrival_rate = in_diff_count
    output_rate = out_diff_count

    thr = 2
    correlation = [0 for _ in range(len(arrival_rate))]
    ave_ratio = sum(output_rate) / sum(arrival_rate)
    print("average ratio: %s" % ave_ratio)
    for i in range(len(arrival_rate)-win):
        if efilter and output_rate[i] > arrival_rate[i] * ave_ratio * thr:
            output_rate[i] = arrival_rate[i] * ave_ratio

    for i in range(len(arrival_rate)-win):
        if arrival_rate[i] > 0:
            correlation[i+win-1] = confidence(arrival_rate, output_rate, i, win)
            correlation[i+win-1] *= 100
        else:
            correlation[i+win-1] = -100

    in_norm = list(arrival_rate)
    out_norm = list(output_rate)

    out_max = max(output_rate)
    for i in range(len(output_rate)):
        out_norm[i] = 100 * output_rate[i] / out_max

    in_max = max(arrival_rate)
    for i in range(len(arrival_rate)):
        in_norm[i] = 100 * arrival_rate[i] / in_max

    for i in range(len(rt_indexes)):
        rt_indexes[i] = i*resolution_in_milli / 1000

    rt_time4plot = list(rt_time)
    rt_max = max(rt_time)
    print("max rt: %s" % rt_max)
    for i in range(len(rt_time4plot)):
        rt_time4plot[i] /= rt_max
    for i in range(1, len(rt_time)):
        if rt_time4plot[i] == 0:
            rt_time4plot[i] = rt_time4plot[i-1]

    corr4plot = list(correlation)
    for i in range(len(corr4plot)):
        corr4plot[i] /= 100
    for i in range(1, len(correlation)):
        if corr4plot[i] < -0.99:
            corr4plot[i] = corr4plot[i-1]
        #if corr4plot[i] > 0.99:
        #    corr4plot[i] = 0
    # e_topic = "Busy Loop"
    x_bound = mon_dur-10
    figg1 = plt.figure(1)
    rt, = plt.plot(rt_indexes, rt_time4plot, '-', color='darkred')
    # plot input, output, correlation
    # in_packet, = plt.plot(axis, in_norm)
    # out_packet, = plt.plot(axis, out_norm)
    # for i in range(len(corr4plot)):
    #     if 60 > axis[i] > 21:
    #         corr4plot[i] -= 0.6
    #     if axis[i] >= 60:
    #         corr4plot[i] = 0
    coefficient, = plt.plot(sample_time, corr4plot, color='steelblue')
    plt.xlabel("time, seconds", fontsize=20)
    # plt.ylabel("egress traffic variance", fontsize=20)
    plt.ylabel("Correlation coefficient", fontsize=20)
    plt.title(e_topic, fontsize=20)
    e_var = [0 for _ in range(len(out_norm))]
    for i in range(9, len(e_var)):
        tmp = list(output_rate[i-9:i+1])
        tmp_norm = np.linalg.norm(tmp)
        for j in range(len(tmp)):
            if tmp_norm > 0:
                tmp[j] /= tmp_norm
            else:
                tmp[j] = 0
        e_var[i] = np.var(tmp)
    e_var_max = max(e_var)
    for i in range(len(e_var)):
        e_var[i] /= e_var_max
    e_var_ave = list(e_var)
    for i in range(9, len(e_var_ave)):
        e_var_ave[i] = np.mean(e_var[i-9:i+1])
    # in_packet, = plt.plot(axis, in_norm, color='darkred')
    # out_packet, = plt.plot(axis, e_var_ave, color='steelblue')
    # plt.legend([rt, out_packet], ["response time", "egress traffic variance"], fontsize=18)
    plt.legend([rt, coefficient], ["client perceived response time", "coefficient of data packets"], fontsize=18,
               bbox_to_anchor=(0., 0.2, 1., .102))
    # plt.plot([30, 30], [0, 1], 'r-')
    # plt.plot([0, 80], [0.6, 0.6], 'r-')
    plt.xlim([0, x_bound])
    plt.ylim([-1.0, 1.0])

    figg2 = plt.figure(2)
    in_ave = list(in_norm)
    e_ave = list(out_norm)
    for i in range(9, len(in_ave)):
        in_ave[i] = np.mean(in_norm[i-9:i+1])
        e_ave[i] = np.mean(out_norm[i-9:i+1])

    ratios = list(arrival_rate)
    for i in range(1, len(ratios)):
        if arrival_rate[i] > 0:
            ratios[i] = output_rate[i] / arrival_rate[i]
        else:
            ratios[i] = ratios[i - 1]
    ratio_ave = list(ratios)
    for i in range(9, len(ratios)):
        ratio_ave[i] = np.mean(ratios[i - 9:i + 1])

    ratio_ave_max = max(ratio_ave)
    for i in range(len(ratio_ave)):
        ratio_ave[i] /= ratio_ave_max

    in_packet, = plt.plot(sample_time, in_norm, color='darkred')
    out_packet, = plt.plot(sample_time, out_norm, color='steelblue')
    print(np.mean(in_norm))
    plt.xlim([0, x_bound])
    plt.xlabel("time, seconds", fontsize=20)
    plt.ylabel("normalized packet count", fontsize=20)
    plt.legend([in_packet, out_packet], ["ingress packet count", "egress packet count"], fontsize=18)
    plt.title(e_topic, fontsize=20)

    figg3 = plt.figure(3)
    var_plot, = plt.plot(sample_time, e_var_ave, color='steelblue')
    # r_plot, = plt.plot(axis, ratio_ave, color='darkred')
    plt.legend([var_plot], ["egress variance"], fontsize=18)#, "ratio"])
    plt.xlim([0, x_bound])
    plt.xlabel("time, seconds", fontsize=20)
    plt.ylabel("Egress Variance", fontsize=20)
    plt.title(e_topic, fontsize=20)

    figg1.savefig("data/%s_corr_index_%s_load_%s_sp_%s.eps"
                 % (e_topic, e_index, e_load, v_resolution),
                 format='eps', dpi=1000)
    figg2.savefig("data/%s_traffic_index_%s_load_%s_sp_%s.eps"
                 % (e_topic, e_index, e_load, v_resolution),
                 format='eps', dpi=1000)
    figg3.savefig("data/%s_evar_index_%s_load_%s_sp_%s.eps"
                 % (e_topic, e_index, e_load, v_resolution),
                 format='eps', dpi=1000)

    figg4 = plt.figure(4)
    bins = np.arange(0, 2000, 20)  # fixed bin size
    plt.hist(rt_time, bins=bins, alpha=0.5, normed=1)
    print(max(rt_time))

    sample_start, cpu_user, cpu_sys, mem_used, ctxt, page_in, page_out, page_fault, swap_in, swap_out, queue = \
        process_sar_data(filename='%s_page_load_%s_exp_%s_dur_%s' % (e_topic, e_load, e_index, e_dur))

    axis = [2 * i for i in range(len(cpu_user))]
    fig1 = plt.figure()
    c_user, = plt.plot(axis, cpu_user)
    c_sys, = plt.plot(axis, cpu_sys)
    m_used, = plt.plot(axis, mem_used)
    plt.legend([c_user, c_sys, m_used], ["cpu_user", "cpu_system", "used memory"])

    fig2 = plt.figure()
    c_txt, = plt.plot(axis, ctxt)
    plt.legend([c_txt], ["context switch"])

    fig3 = plt.figure()
    p_in, = plt.plot(axis, page_in)
    plt.legend([p_in], ["number of page in"])

    fig4 = plt.figure()
    p_out, = plt.plot(axis, page_out)
    plt.legend([p_out], ["number of page out"])

    fig5 = plt.figure()
    s_in, = plt.plot(axis, swap_in)
    plt.legend([s_in], ["number of swapped pages in"])

    fig6 = plt.figure()
    s_out, = plt.plot(axis, swap_out)
    plt.legend([s_out], ["number of swapped pages out"])

    fig7 = plt.figure()
    p_fault, = plt.plot(axis, page_fault)
    plt.legend([p_fault], ["number of page faults"])

    fig8 = plt.figure()
    q_queue, = plt.plot(axis, queue)
    plt.legend([q_queue], ["number of waiting threads"])

    cpu_user_gen = data_point_gen(ctxt)
    dists = [0 for _ in range(len(cpu_user_gen))]
    for i in range(len(cpu_user_gen)):
        set_index = max(0, i-10)
        dists[i] = dist2neighbors(cpu_user_gen[set_index:set_index+10], cpu_user_gen[i], 5)

    fig9 = plt.figure()
    plt.plot(dists)

    if 0:
        print("detection lags: ")
        searches = [410, 760, 880, 1120, 1360, 1850, 2440, 3040, 3400]
        lag_total = 0
        lag_count = 0
        for center in searches:
            lag = lag_find(sample_time, rt_indexes, corr4plot, rt_time, 0.07, center)
            if 20 >= lag >= 0:
                lag_total += lag
                lag_count += 1
            print(lag)

        ave_lag = lag_total/lag_count
        print("average lag: %s " % ave_lag)

    plt.show()
    plt.close("all")

if __name__ == '__main__':
    sampling_period = 0.2
    mon_dur = 90
    dur = 90

    indexes = [i for i in range(0, 1)]
    topics = ["Normal"]  # , "Bursty Workload", "CPU cap"]  # , "exp", "hv"]
    loads = [0.04, 0.11]

    interf_time = []
    burst_time = []
    resize_time = []
    net_time = []
    net_hog_time = []
    cpu_hog_time = []
    mem_hog_time = []
    io_hog_time = []

    for k in range(len(topics)):
        for index in indexes:
            print("index: %s " % index)
            topic = topics[k]
            traffic_load = 0.03
            mode = '_hv'
            corwin = 40
            experiment(e_load=traffic_load, monitor_len=mon_dur, e_dur=dur, ini=0.03, col=False, win=corwin,
                       resolution=sampling_period, e_index=index, e_topic=topic, efilter=True, inject_dur=20,
                       interf_time=interf_time, burst_time=burst_time, resize_time=resize_time, net_time=net_time,
                       net_hog_time=net_hog_time, cpu_hog_time=cpu_hog_time, mem_hog_time=mem_hog_time,
                       io_hog_time=io_hog_time)

