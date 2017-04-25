from threading import Thread
import matplotlib.pyplot as plt
from util import *

__author__ = 'Ke Wang'
__copyright__ = 'Copyright 2017, Ke Wang'
__version__ = '0.6'
__email__ = 'kewang1@andrew.cmu.edu'


def experiment(monitor_len, resolution=1.0, e_index=0, e_topic='', col=False, win=10,
               efilter=False, cpu_dur=20, cpu_sleep_dur=40, cpu_bound=15, cpu_flag=False, net_flag=False):
    if col:
        # need to collect raw data
        # launch workload generator
        t_generator = Thread(target=generate_traffic)
        print("Start network_monitor thread")
        t_network_monitor = Thread(target=network_monitor, kwargs={'duration': monitor_len, 'reso': resolution,
                                                                   'output_file': '%s_network_exp_%s'
                                                                                  % (e_topic, e_index)})
        print("Experimenting...")
        t_network_monitor.start()
        t_generator.start()
        if cpu_flag:
            # t_cpu_insert = Thread(target=cpu_insert, kwargs={'duration': cpu_dur, 'bound': cpu_bound,
            #                                                  'sleep_dur': cpu_sleep_dur})

            t_cpu_insert = Thread(target=cpu_vm, kwargs={'duration': cpu_dur, 'sleep_dur': cpu_sleep_dur})
            # t_cpu_insert = Thread(target=cpu_resize, kwargs={'duration': cpu_dur, 'sleep_dur': cpu_sleep_dur})
            t_cpu_insert.start()
            t_cpu_insert.join()
        elif net_flag:
            # print("%s, %s" % (cpu_dur, cpu_sleep_dur))
            # t_net_insert = Thread(target=lossy_network, kwargs={'duration': cpu_dur, 'sleep_dur': cpu_sleep_dur})
            t_net_insert = Thread(target=network_insert, kwargs={'duration': cpu_dur, 'sleep_dur': cpu_sleep_dur,
                                                                 'src_ip': "10.1.1.9"})
            t_net_insert.start()
            # t_net_insert2.start()
            t_net_insert.join()
            # t_net_insert2.join()
        t_network_monitor.join()
        t_generator.join()
        print("Finishing gathering data, begin processing...")

    sample_time, in_diff_count, out_diff_count, sum_in, sum_out = process_network_data(topic=e_topic, index=e_index)

    arrival_rate = in_diff_count
    output_rate = out_diff_count
    rt_start = sample_time[0]
    for i in range(len(in_diff_count)):
        sample_time[i] -= rt_start
        sample_time[i] /= 1000

    # first filter always applied: avoid unexpected drop of traffic
    for i in range(1, len(arrival_rate)):
        if 0 and (output_rate[i]-output_rate[i-1]) < -250 and sample_time[i] < 70:
            arrival_rate[i] = arrival_rate[i-1]
            output_rate[i] = output_rate[i-1]

    for i in range(1, len(arrival_rate)-1):
        if 0 and (output_rate[i] - output_rate[i + 1]) < -300 and 10 < sample_time[i] < 80:
            arrival_rate[i] = arrival_rate[i - 1]
            output_rate[i] = output_rate[i - 1]

    thr_upper = 2
    thr_lower = 0.5
    correlation = [0 for _ in range(len(arrival_rate))]
    get_arr = 0
    get_out = 0
    for i in range(len(arrival_rate)):
        if sample_time[i] < 35 and arrival_rate[i] < 50:
            get_arr += arrival_rate[i]
            get_out += output_rate[i]

    true_ratio = 11  # get_out / get_arr
    print("Actual get request ratio: %s" % true_ratio)

    # second filter: filter out POST requests.
    # for i in range(len(arrival_rate)):
    #    if output_rate[i] < arrival_rate[i] * true_ratio * thr_lower:
    #         arr_m[i] = output_rate[i] / true_ratio

    # third filter: egress traffic filter
    for i in range(len(arrival_rate)):
        if efilter and output_rate[i] > arrival_rate[i] * true_ratio * thr_upper:
            output_rate[i] = arrival_rate[i] * true_ratio
        if 0 and efilter and output_rate[i] < arrival_rate[i] * true_ratio * thr_lower:
            arrival_rate[i] = output_rate[i] / true_ratio

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

    corr4plot = list(correlation)
    for i in range(len(corr4plot)):
        corr4plot[i] /= 100
    for i in range(1, len(correlation)):
        if corr4plot[i] < -0.99:
            corr4plot[i] = corr4plot[i-1]

    x_bound = monitor_len + 10
    fig1 = plt.figure(1)
    coefficient, = plt.plot(sample_time, corr4plot, color='steelblue')
    plt.xlabel("time, seconds", fontsize=20)
    # plt.ylabel("egress traffic variance", fontsize=20)
    plt.ylabel("Correlation coefficient", fontsize=20)
    plt.title(e_topic, fontsize=20)
    plt.xlim([0, x_bound])
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
    # plt.legend([rt, coefficient], ["response time", "coefficient"], fontsize=18)
    # plt.legend([coefficient, coefficient_m], ["with post traffic", "without post traffic"],
    #            fontsize=18, bbox_to_anchor=(0., 0.2, 1., .102))
    # plt.plot([30, 30], [0, 1], 'r-')
    # plt.plot([0, 80], [0.6, 0.6], 'r-')
    plt.xlim([0, x_bound])
    plt.ylim([-1.0, 1.0])

    fig2 = plt.figure(2)
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
    plt.xlim([0, x_bound])
    plt.xlabel("time, seconds", fontsize=20)
    plt.ylabel("normalized packet count", fontsize=20)
    plt.legend([in_packet, out_packet], ["ingress packet count", "egress packet count"], fontsize=18)
    plt.title(e_topic, fontsize=20)

    fig3 = plt.figure(3)
    var_plot, = plt.plot(sample_time, e_var_ave, color='steelblue')
    # r_plot, = plt.plot(sample_time, ratios, color='darkred')
    plt.legend([var_plot], ["egress variance"], fontsize=18)#, "ratio"])
    # plt.legend([r_plot], ["egress variance"], fontsize=18)
    plt.xlim([0, x_bound])
    plt.xlabel("time, seconds", fontsize=20)
    plt.ylabel("ratio of egress over ingress traffic", fontsize=20)
    plt.title(e_topic, fontsize=20)

    fig1.savefig("%s_corr_index_%s.eps" % (e_topic, e_index), format='eps', dpi=1000)
    fig2.savefig("%s_traffic_index_%s.eps" % (e_topic, e_index), format='eps', dpi=1000)
    #fig3.savefig("%s_evar_index_%s.eps" % (e_topic, e_index), format='eps', dpi=1000)

    plt.show()

    plt.close("all")

if __name__ == '__main__':
    s_period = 0.2
    mon_dur = 60

    indexes = [i for i in range(0, 1)]
    topics = ["CPU Interference"]  # , "Bursty Workload", "CPU cap"]  # , "exp", "hv"]
    for k in range(len(topics)):
        for index in indexes:
            topic = topics[k]
            corwin = 40
            experiment(monitor_len=mon_dur, col=False, win=corwin, resolution=s_period, e_index=index, e_topic=topic,
                       efilter=False, cpu_dur=35, cpu_sleep_dur=35, cpu_bound=25, cpu_flag=False, net_flag=False)

