import unittest
import collections
import shutil
import os

import config
from workflow import *
import wiscsim
from utilities import utils
from wiscsim.hostevent import Event, ControlEvent
from config_helper import rule_parameter
from pyreuse.helpers import shcmd
from config_helper import experiment


def create_config():
    #conf = wiscsim.dftldes.Config()
    conf = wiscsim.nkftl2.Config()
    conf['SSDFramework']['ncq_depth'] = 1

    conf['flash_config']['n_pages_per_block'] = 256
    conf['flash_config']['n_blocks_per_plane'] = 2048
    conf['flash_config']['n_planes_per_chip'] = 4
    conf['flash_config']['n_chips_per_package'] = 4
    conf['flash_config']['n_packages_per_channel'] = 1
    conf['flash_config']['n_channels_per_dev'] = 8

    # set ftl
    conf['do_not_check_gc_setting'] = True
    conf.GC_high_threshold_ratio = 0.96
    conf.GC_low_threshold_ratio = 0

    conf['enable_simulation'] = True

    utils.set_exp_metadata(conf, save_data = False,
            expname = 'test_expname',
            subexpname = 'test_subexpname')

    conf['ftl_type'] = 'nkftl2'
    conf['simulator_class'] = 'SimulatorDESNew'

    logicsize_mb = 16
    #conf.n_cache_entries = conf.n_mapping_entries_per_page * 16
    #conf.set_flash_num_blocks_by_bytes(int(logicsize_mb * 2**20 * 1.28))

    utils.runtime_update(conf)

    return conf


def on_fs_config(conf):
    # environment
    conf['device_path'] = "/dev/loop0"
    conf['dev_size_mb'] = 16
    conf['filesystem'] = "ext4"
    conf["n_online_cpus"] = 'all'

    conf['linux_ncq_depth'] = 31

    # workload
    conf['workload_class'] = 'SimpleRandReadWrite'

class TestWorkflow(unittest.TestCase):
    def test_init(self):
        conf = create_config()
        wf = Workflow(conf)

    def test_save_conf(self):
        conf = create_config()
        conf['result_dir'] = '/tmp/'
        jsonpath = os.path.join(conf['result_dir'], 'config.json')

        if os.path.exists(jsonpath):
            os.remove(jsonpath)

        wf = Workflow(conf)
        wf._save_conf()

        self.assertTrue(os.path.exists(jsonpath))


    def test_onfs_workload(self):
        conf = create_config()
        on_fs_config(conf)

        datapath = os.path.join(conf["fs_mount_point"], 'datafile')
        if os.path.exists(datapath):
            os.remove(datapath)

        wf = Workflow(conf)
        wf.run_workload()

        self.assertTrue(os.path.exists(datapath))

    def test_simulation(self):
        conf = create_config()

        ctrl_event = ControlEvent(OP_ENABLE_RECORDER)
        events = []
        events += [Event(512, 0, OP_WRITE, i*4096, 4096) for i in reversed(range(257))]


        wf = Workflow(conf)
        sim = wf.run_simulator([ctrl_event]+events)
        # dftl print
        # print([row for row in sim.ssd.ftl._mappings._lpn_table.rows() if row.dirty])
        # nkftl print
        # print([(lpn, ppn) \
        #     for dgn, log_group in sim.ssd.ftl.log_mapping_table.log_group_info.items() \
        #     for lpn, ppn in log_group._page_map])
        # print(conf)
        print([log_group._page_map \
            for dgn, log_group in sim.ssd.ftl.log_mapping_table.log_group_info.items() ])

        print(sim.ssd.ftl.data_block_mapping_table.logical_to_physical_block)

    def test_on_fs_run_and_sim(self):
        conf = create_config()
        on_fs_config(conf)
        conf['enable_blktrace'] = True

        datapath = os.path.join(conf["fs_mount_point"], 'datafile')
        if os.path.exists(datapath):
            os.remove(datapath)

        wf = Workflow(conf)
        wf.run()

        self.assertTrue(os.path.exists(datapath))



if __name__ == '__main__':
    unittest.main()

