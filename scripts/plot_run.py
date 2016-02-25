#!/usr/bin/env python

import argparse
import collections
import datetime
import matplotlib as mpl
import matplotlib.pyplot as plt
import matplotlib.ticker as ticker


def unix_time(dt):
    epoch = datetime.datetime.utcfromtimestamp(0)
    delta = dt - epoch
    return delta.total_seconds()


def unix_time_millis(dt):
    return unix_time(dt) * 1000.0


def parse_datetime(text):
    return datetime.datetime.strptime(text, "%Y-%m-%d %H:%M:%S.%f")


def to_seconds(ms):
    return ms / 1000.0


def split_ip(ip):
    lang_sep = ip.index('_')
    return tuple(int(part) for part in ip[:lang_sep].split('.'))


def dpe_key(item):
    return split_ip(item)


class File:
    def __init__(self, name, events, node):
        self.node = node
        self.name = name
        self.events = events

    def start(self, time):
        self.t_start = time

    def end(self, time):
        self.t_end = time

    def avg(self, avg):
        self.rec_avg = avg

    def deltas(self, u_start):
        self.d_start = unix_time_millis(self.t_start) - u_start
        self.d_end = unix_time_millis(self.t_end) - u_start
        self.d_total = self.d_end - self.d_start


class Node:
    def __init__(self, name, time):
        self.name = name
        self.t_start = time
        self.files = collections.OrderedDict()

    def add_file(self, infile):
        self.files[infile.name] = infile

    def delta(self, g_start):
        self.d_start = unix_time_millis(self.t_start) - g_start
        self.d_ready = unix_time_millis(self.t_ready) - g_start

    def ready(self, time):
        self.t_ready = time

    def report(self):
        print self.name
        for n, f in self.files.items():
            print "  ", n, f.events, f.rec_avg


class Parser:

    def __init__(self):
        self.nodes = collections.OrderedDict()
        self.files = collections.OrderedDict()

        self.t_start = None
        self.t_end = None
        self.u_start = None
        self.u_end = None

    def parse(self, log_file):
        with open(log_file) as f:
            for line in f:
                if "Start processing" in line:
                    self._parse_start_node(line)
                elif "All services deployed" in line:
                    self._parse_node_ready(line)
                elif "Using" in line:
                    self._parse_start_file(line)
                elif "Finished" in line or "reconstructed!" in line:
                    self._parse_end_file(line)
                elif "Waiting for" in line:
                    self.t_start = self._get_logtime(line.split())
                    self.u_start = unix_time_millis(self.t_start)
                elif "Global average" in line:
                    self.t_end = self._get_logtime(line.split())
                    self.u_end = unix_time_millis(self.t_end)

        for n in self.nodes.values():
            n.delta(self.u_start)

        for f in self.files.values():
            f.deltas(self.u_start)

    def report(self):
        for k in sorted(self.nodes, key=dpe_key):
            n = self.nodes[k]
            nd = n.t_start.strftime("%H:%M:%S.%f")[:-3]
            print "[%s]:  %s" % (nd, n.name)
            for f in n.files.values():
                d = f.t_start.strftime("%H:%M:%S.%f")[:-3]
                fmt = '    [%s]: %s,  total = %5d [ms],  avg = %5.2f [ms]'
                print fmt % (d, f.name, f.d_total, f.rec_avg)

    def _parse_start_node(self, line):
        tokens = line.split()
        time = self._get_logtime(tokens)
        ip = tokens[5][:-3]
        self.nodes[ip] = Node(ip, time)

    def _parse_node_ready(self, line):
        tokens = line.split()
        time = self._get_logtime(tokens)
        ip = tokens[-1]
        node = self.nodes[ip]
        node.ready(time)

    def _parse_start_file(self, line):
        tokens = line.split()
        time = self._get_logtime(tokens)
        ip = tokens[6]
        events = tokens[9]
        name = tokens[-1]

        node = self.nodes[ip]
        infile = File(name, events, node)
        infile.start(time)

        self.files[name] = infile
        node.add_file(infile)

    def _parse_end_file(self, line):
        tokens = line.split()
        time = self._get_logtime(tokens)
        if tokens[2] == "Finished":
            name = tokens[4]
        else:
            name = tokens[3]
        avg = tokens[-2]

        infile = self.files[name]
        infile.end(time)
        infile.avg(float(avg))

    def _get_logtime(self, tokens):
        t = ' '.join(tokens[0:2])[:-1]
        return parse_datetime(t)


class Plot:

    def __init__(self):
        self.fig = plt.figure()
        self.ax = self.fig.add_subplot(111)

    def fill(self, parser):
        plt.ylim([0, len(parser.nodes)])
        plt.xlim([0, to_seconds(parser.u_end - parser.u_start)])

        self._draw_nodes(parser.nodes)
        self._draw_files(parser.files)

    def show(self):
        plt.show()

    def _draw_nodes(self, nodes):
        self.nodes_pos = {}
        i = 0
        for k in reversed(sorted(nodes, key=dpe_key)):
            n = nodes[k]
            i += 1
            pos = len(nodes) - i
            self.nodes_pos[n.name] = pos

            node_start = to_seconds(n.d_start)
            node_ready = to_seconds(n.d_ready)

            self.ax.axhline(y=i, color='black')
            plt.plot((node_start, node_start), (pos, pos+1), 'r-')
            plt.plot((node_ready, node_ready), (pos, pos+1), 'r-')

        ytpos = [y + 0.5 for y in range(len(nodes))]
        ytnames = [nodes[k].name for k in sorted(nodes, key=dpe_key)]
        self.ax.yaxis.set_major_formatter(ticker.NullFormatter())
        self.ax.yaxis.set_minor_locator(ticker.FixedLocator(ytpos))
        self.ax.yaxis.set_minor_formatter(ticker.FixedFormatter(ytnames))

    def _draw_files(self, files):
        self.ax.set_xlabel("Time [s]")
        start, end = self.ax.get_xlim()
        for f in files.values():
            pos = self.nodes_pos[f.node.name]
            point = (to_seconds(f.d_start), pos)
            width = to_seconds(f.d_total)
            height = 1
            rect = mpl.patches.Rectangle(point, width, height, color='red')
            self.ax.add_patch(rect)


if __name__ == '__main__':

    argparser = argparse.ArgumentParser()
    argparser.add_argument('log_file',
                           help='the cloud-orchestrator output')
    args = argparser.parse_args()

    parser = Parser()
    parser.parse(args.log_file)
    parser.report()

    plot = Plot()
    plot.fill(parser)
    plot.show()
