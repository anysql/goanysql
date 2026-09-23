import time
import os
import argparse
import socket
#import psutil

RED = "\033[31m"
GREEN = "\033[32m"
YELLOW = "\033[33m"
RESET = "\033[0m"

def getlocalip():
    hostname = socket.gethostname()
    local_ip = socket.gethostbyname(hostname)
    return ".".join(local_ip.split(".")[-3:])

def parse_tcp_proc_file(filepath):
    """Parses a /proc/net/tcp file and yields (send_q, recv_q, st, tr, tr->when) for each connection."""
    if not os.path.exists(filepath):
        return

    with open(filepath, 'r') as f:
        # Skip the header line
        next(f, None)

        for line in f:
            parts = line.strip().split()
            if len(parts) < 5:
                continue

            # The 5th column (index 4) contains "tx_queue:rx_queue" in hex format
            # Example: "00000000:00000000"
            try:
                tx_hex, rx_hex = parts[4].split(':')
                send_q = int(tx_hex, 16)
                recv_q = int(rx_hex, 16)
                st = int(parts[3], 16)
                tr_hex, tm_hex = parts[5].split(':')
                tr = int(tr_hex, 16)
                tm = int(tm_hex, 16)
                yield send_q, recv_q, st, tr, tm
            except (ValueError, IndexError):
                continue

def read_metrics():
    metrics = {
        "TcpExt": {},
        "InSegs": 0,
        "OutSegs": 0,
        "RetransSegs":0,
        "NICRxDrops":0,
        "NICTxDrops":0
    }

    try:
        with open("/proc/net/netstat", "r") as f:
            headers = None
            for line in f:
                if not line.startswith("TcpExt:"):
                    continue
                fields = line.strip().split()[1:]
                if headers is None:
                    headers = fields
                else:
                    metrics["TcpExt"] = dict(zip(headers, [int(x) for x in fields]))
                    break
    except FileNotFoundError:
        pass

    try:
        with open("/proc/net/snmp", "r") as f:
            tcp_headers = None
            for line in f:
                if not line.startswith("Tcp:"):
                    continue
                fields = line.strip().split()[1:]
                if tcp_headers is None:
                    tcp_headers = fields
                else:
                    snmp_dict = dict(zip(tcp_headers, fields))
                    metrics["InSegs"] = int(snmp_dict.get("InSegs", 0))
                    metrics["OutSegs"] = int(snmp_dict.get("OutSegs", 0))
                    metrics["RetransSegs"] = int(snmp_dict.get("RetransSegs", 0))
                    break
    except FileNotFoundError:
        pass

    # Automatically fetches and iterates through all available interfaces
    nic_rx_drop = 0
    nic_tx_drop = 0
    #net_stats = psutil.net_io_counters(pernic=True)
    #for interface, stats in net_stats.items():
    #    nic_rx_drop += stats.dropin
    #    nic_tx_drop += stats.dropout

    metrics["NICRxDrops"] = nic_rx_drop
    metrics["NICTxDrops"] = nic_tx_drop

    return metrics

def get_delta(curr, prev):
    if curr >= prev:
        return curr - prev
    return curr

def get_val_ptr(val):
    if val < 10000:
        return str(val)
    val = int(val / 1000)
    return str(val)+"K"

def get_color_pct(pct, pstr):
    if pct < 0.1:
        return GREEN+pstr+RESET
    if pct < 1.0:
        return YELLOW+pstr+RESET
    return RED+pstr+RESET

def get_pct_str(delta, base):
    if base == 0:
        return "0.000"
    pct = (delta / base) * 100
    pstr = f"{pct:.3f}"[0:5]
    return get_color_pct(pct, pstr)

def get_jiltertime(val, hz):
    tim = (1 * val) / hz
    return f"{tim:.3f}"[0:4].removesuffix(".")

def main():
    parser = argparse.ArgumentParser(
        description="TCP Quality Monitor Command Line Arguments."
    )

    parser.add_argument(
        "-i", "--interval",
        type=int,
        default=10,
        help="Interval in seconds (default: 10)"
    )

    args = parser.parse_args()

    rows = 0
    localip = getlocalip()
    cpuhz = os.sysconf(os.sysconf_names['SC_CLK_TCK'])
    print(f"Starting TCP Quality Monitor (Interval: {args.interval}s)...")

    # Formatted explicitly to prevent separate % symbol tracking shift artifacts
    header_fmt = "{:<19} | {:11}| {:<3} | {:<3} | {:<6} | {:<4} | {:<4} | {:<4} | {:<8} | {:<8} | {:<6} | {:<5} | {:<4} | {:<5} | {:<4} | {:<5} | {:<4} | {:<5} | {:<4} | {:<5} | {:<4} | {:<5} | {:<4}"
    row_fmt    = "{:<19} | {:<11}| {:<3d} | {:<3d} | {:<6d} | {:<4} | {:<4} | {:<4} | {:<8} | {:<8} | {:<6} | {:<5} | {:<4} | {:<5} | {:<4} | {:<5} | {:<4} | {:<5} | {:<4} | {:<5} | {:<4} | {:<5} | {:<4}"

    proc_files = {
        'IPv4 TCP': '/proc/net/tcp',
        'IPv6 TCP': '/proc/net/tcp6'
    }

    prev = read_metrics()
    start_time = time.time()
    elapsed_time = 0

    try:
        while True:
            elapsed_time = time.time() - start_time
            time.sleep(args.interval - elapsed_time)
            start_time = time.time()
            curr = read_metrics()

            non_zero_recv = 0
            non_zero_send = 0
            total_tcp_conns = 0
            trans_wait_time = 0
            probe_wait_time = 0
            trans_wait_maxt = 0
            for label, filepath in proc_files.items():
                file_recv = 0
                file_send = 0
                for send_q, recv_q, st, tr, tm in parse_tcp_proc_file(filepath):
                    if st != 1 or tr == 3:
                        continue
                    if recv_q > 0:
                        file_recv += 1
                    if send_q > 0:
                        file_send += 1
                    total_tcp_conns += 1
                    if tr == 1:
                        trans_wait_time += tm
                        trans_wait_maxt = max(trans_wait_maxt, tm)
                    if tr == 4:
                        probe_wait_time += tm
                non_zero_recv += file_recv
                non_zero_send += file_send

            timestamp = time.strftime("%Y-%m-%d %H:%M:%S")
            if rows % 20 == 0:
                print(header_fmt.format(
                    timestamp, "IP", "RX‰", "TX‰", "Total", "AckW", "MaxW", "PrbW", "TxSegs", "RxSegs", "Retr", "Retr%", "Fail", "Fail%", "TimO", "TimO%", "Drop", "Drop%", "OfoR", "OfoR%", "OfoT", "OfoT%", "Zero"
                ))
                print("-" * 196)
            rows = rows + 1

            out_delta = get_delta(curr["OutSegs"], prev["OutSegs"])
            in_delta = get_delta(curr["InSegs"], prev["InSegs"])
            retrans_delta = get_delta(curr["RetransSegs"], prev["RetransSegs"])
            rx_drop_delta = get_delta(curr["NICRxDrops"], prev["NICRxDrops"])
            tx_drop_delta = get_delta(curr["NICTxDrops"], prev["NICTxDrops"])

            tcp_ext_curr = curr["TcpExt"]
            tcp_ext_prev = prev["TcpExt"]

            rcv_pruned = get_delta(tcp_ext_curr.get("RcvPruned", 0), tcp_ext_prev.get("RcvPruned", 0))
            ofo_pruned = get_delta(tcp_ext_curr.get("OfoPruned", 0), tcp_ext_prev.get("OfoPruned", 0))
            all_pruned = rcv_pruned + rcv_pruned

            #ack_delayed = get_delta(tcp_ext_curr.get("DelayedACKs", 0), tcp_ext_prev.get("DelayedACKs", 0))
            #ack_delaydrop = get_delta(tcp_ext_curr.get("DelayedACKLost", 0), tcp_ext_prev.get("DelayedACKLost", 0))
            #all_ackdelay = int(100 * ack_delaydrop / (ack_delayed + ack_delaydrop + 1))

            #fast_retrans = get_delta(tcp_ext_curr.get("TCPFastRetrans", 0), tcp_ext_prev.get("TCPFastRetrans", 0))
            #slow_retrans = get_delta(tcp_ext_curr.get("TCPSlowStartRetrans", 0), tcp_ext_prev.get("TCPSlowStartRetrans", 0))
            #syn_retrans = get_delta(tcp_ext_curr.get("TCPSynRetrans", 0), tcp_ext_prev.get("TCPSynRetrans", 0))
            #total_retransm = fast_retrans + slow_retrans + syn_retrans
            orig_datasent = get_delta(tcp_ext_curr.get("TCPOrigDataSent", 0), tcp_ext_prev.get("TCPOrigDataSent", 0))
            total_retransm = retrans_delta

            lost_retrans = get_delta(tcp_ext_curr.get("TCPLostRetransmit", 0), tcp_ext_prev.get("TCPLostRetransmit", 0))
            fail_retrans = get_delta(tcp_ext_curr.get("TCPRetransFail", 0), tcp_ext_prev.get("TCPRetransFail", 0))
            total_retransfail = lost_retrans + fail_retrans

            timeouts = get_delta(tcp_ext_curr.get("TCPTimeouts", 0), tcp_ext_prev.get("TCPTimeouts", 0))
            loss_probes = get_delta(tcp_ext_curr.get("TCPLossProbes", 0), tcp_ext_prev.get("TCPLossProbes", 0))

            backlog_drop = get_delta(tcp_ext_curr.get("TCPBacklogDrop", 0), tcp_ext_prev.get("TCPBacklogDrop", 0))
            memalloc_drop = get_delta(tcp_ext_curr.get("PFMemallocDrop", 0), tcp_ext_prev.get("PFMemallocDrop", 0))
            accept_drop = get_delta(tcp_ext_curr.get("TCPDeferAcceptDrop", 0), tcp_ext_prev.get("TCPDeferAcceptDrop", 0))
            sendfull_drop = get_delta(tcp_ext_curr.get("TCPReqQFullDrop", 0), tcp_ext_prev.get("TCPReqQFullDrop", 0))
            recvfull_drop = get_delta(tcp_ext_curr.get("TCPRcvQDrop", 0), tcp_ext_prev.get("TCPRcvQDrop", 0))
            zerowind_drop = get_delta(tcp_ext_curr.get("TCPZeroWindowDrop", 0), tcp_ext_prev.get("TCPZeroWindowDrop", 0))
            ofo_drop = get_delta(tcp_ext_curr.get("TCPOFODrop", 0), tcp_ext_prev.get("TCPOFODrop", 0))
            total_indrop = backlog_drop + memalloc_drop + accept_drop + sendfull_drop + recvfull_drop + zerowind_drop + ofo_drop + rx_drop_delta + tx_drop_delta
            ofo_queue = get_delta(tcp_ext_curr.get("TCPOFOQueue", 0), tcp_ext_prev.get("TCPOFOQueue", 0)) #receive reorder
            sack_reorder = get_delta(tcp_ext_curr.get("TCPSACKReorder", 0), tcp_ext_prev.get("TCPSACKReorder", 0)) #send reorder

            # Extract zero window advertisements (indicates window scale or system buffering stress)
            to_zero_win = get_delta(tcp_ext_curr.get("TCPToZeroWindowAdv", 0), tcp_ext_prev.get("TCPToZeroWindowAdv", 0))
            from_zero_win = get_delta(tcp_ext_curr.get("TCPFromZeroWindowAdv", 0), tcp_ext_prev.get("TCPFromZeroWindowAdv", 0))
            want_zero_win = get_delta(tcp_ext_curr.get("TCPWantZeroWindowAdv", 0), tcp_ext_prev.get("TCPWantZeroWindowAdv", 0))

            #timestamp = time.strftime("%Y-%m-%d %H:%M:%S")
            print(row_fmt.format(
                timestamp, localip,
                int(1000 * non_zero_recv / (total_tcp_conns + 1)),
                int(1000 * non_zero_send / (total_tcp_conns + 1)), total_tcp_conns,
                get_jiltertime(trans_wait_time, cpuhz),
                get_jiltertime(trans_wait_maxt, cpuhz),
                get_jiltertime(probe_wait_time, cpuhz),
                get_val_ptr(int(out_delta/args.interval)), get_val_ptr(int(in_delta/args.interval)),
                #int(total_retransm/args.interval), get_pct_str(total_retransm, out_delta),
                get_val_ptr(int(total_retransm/args.interval)), get_pct_str(total_retransm, total_retransm + orig_datasent),
                get_val_ptr(int(total_retransfail/args.interval)), get_pct_str(total_retransfail, out_delta),
                get_val_ptr(int((timeouts + loss_probes)/args.interval)), get_pct_str(timeouts + loss_probes, out_delta),
                get_val_ptr(int(total_indrop/args.interval)), get_pct_str(total_indrop, in_delta),
                get_val_ptr(int(ofo_queue/args.interval)), get_pct_str(ofo_queue, in_delta),
                get_val_ptr(int(sack_reorder/args.interval)), get_pct_str(sack_reorder, out_delta),
                get_val_ptr(int((to_zero_win + from_zero_win + want_zero_win)/args.interval))
            ))

            prev = curr

    except KeyboardInterrupt:
        print("\nMonitoring stopped.")

if __name__ == "__main__":
    main()

