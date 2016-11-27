"""
Copyright (c) 2014 Maciej Nabozny
              2015 Marta Nabozny

This file is part of CloudOver project.

CloudOver is free software: you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation, either version 3 of the License, or
(at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with this program.  If not, see <http://www.gnu.org/licenses/>.
"""

import libvirt
import re
import time

from corecluster.models.core.vm import VM
from corecluster.agents.storage_libvirt import AgentThread as StorageAgent
from corecluster.agents.base_agent import BaseAgent
from corecluster.exceptions.agent import *
from corenetwork.utils.logger import log
from corenetwork.utils import system, config


class AgentThread(BaseAgent):
    node = None
    task_type = 'node'
    supported_actions = ['load_image', 'delete', 'save_image', 'mount', 'umount', 'create_images_pool', 'check', 'suspend', 'wake_up', 'resize_image']


    def get_storage(self, name, conn):
        try:
            storage = conn.storagePoolLookupByName(name)
        except Exception as e:
            raise TaskFatalError("node_storage_not_found", exception=e)

        if storage.info()[0] != libvirt.VIR_STORAGE_POOL_RUNNING:
            raise TaskFatalError("node_storage_not_running")

        storage.refresh(0)
        return storage


    def load_image(self, task):
        node = task.get_obj('Node')

        node.check_online(task.ignore_errors)

        image = task.get_obj('Image')
        vm = task.get_obj('VM')

        conn = node.libvirt_conn()
        if image.state != 'ok':
            raise TaskNotReady('image_wrong_state')

        src_storage = self.get_storage(image.storage.name, conn)
        dest_storage = self.get_storage('images', conn)

        try:
            base_volume = src_storage.storageVolLookupByName(image.libvirt_name)
        except Exception as e:
            conn.close()
            raise TaskError('node_load_image_not_found', e)

        new_volume_xml = base_volume.XMLDesc(0)
        new_volume_xml = new_volume_xml.replace('<name>%s</name>' % (image.libvirt_name),
                                                '<name>%s</name>' % str(vm.id))
        new_volume_xml = re.sub(r'<group>[0-9\.]+</group>', '', new_volume_xml)
        new_volume_xml = re.sub(r'<owner>[0-9\.]+</owner>', '', new_volume_xml)

        try:
            dest_storage.createXMLFrom(new_volume_xml, base_volume, 0)
        except Exception as e:
            vm.set_state('failed')
            vm.save()
            conn.close()
            raise TaskFatalError('node_load_image_failed', exception=e)

        conn.close()


    def delete(self, task):
        '''
        Delete volume
        '''
        node = task.get_obj('Node')
        node.check_online(task.ignore_errors)
        vm = task.get_obj('VM')
        if vm.state not in ['stopped', 'closed', 'closing'] and not task.ignore_errors:
            raise TaskNotReady('vm_not_stopped')

        conn = node.libvirt_conn()

        try:
            storage = self.get_storage('images', conn)
        except Exception as e:
            log(msg="Cannot get images storage", exception=e, tags=('error', 'agent', 'node'), context=task.logger_ctx)
            conn.close()
            raise TaskError('node_storage_get')
        try:
            volume = storage.storageVolLookupByName("%s" % (vm.id))
            volume.delete(0)
        except Exception as e:
            log(msg="Image %s not found. Skipping" % vm.id, exception=e, tags=('alert', 'agent', 'node'), context=task.logger_ctx)

        conn.close()


    def save_image(self, task):
        node = task.get_obj('Node')

        node.check_online(task.ignore_errors)

        vm = task.get_obj('VM')
        image = task.get_obj('Image')
        if not vm.in_state('stopped'):
            raise TaskNotReady('vm_not_stopped')

        vm.set_state('saving')
        vm.save()

        conn = node.libvirt_conn()

        dest_storage = self.get_storage(image.storage.name, conn)
        src_storage = self.get_storage('images', conn)

        new_volume_xml = image.libvirt_xml()
        try:
            base_volume = src_storage.storageVolLookupByName('%s' % vm.id)
        except Exception as e:
            conn.close()
            raise TaskError('node_save_vm_image_not_found', exception=e)

        try:
            dest_storage.createXMLFrom(new_volume_xml, base_volume, 0)
        except Exception as e:
            conn.close()
            raise TaskError('node_image_save', exception=e)

        vm.set_state('stopped')
        vm.save()

        image.size = base_volume.info()[1]
        image.set_state('ok')
        image.save()

        conn.close()


    def resize_image(self, task):
        vm = task.get_obj('VM')

        vm.node.check_online(task.ignore_errors)

        if not vm.in_states(['stopped']):
            raise TaskError('vm_not_stopped')

        image_size = int(task.get_prop('size'))

        if image_size > vm.template.hdd*1024*1024:
            raise TaskError('vm_resize_over_template')

        conn = vm.node.libvirt_conn()
        pool = conn.storagePoolLookupByName('images')
        vol = pool.storageVolLookupByName(vm.id)
        vol.resize(image_size)


    def mount(self, task):
        node = task.get_obj('Node')
        conn = node.libvirt_conn()
        StorageAgent.real_mount(task, conn)
        conn.close()


    def umount(self, task):
        node = task.get_obj('Node')
        conn = node.libvirt_conn()
        node.state = 'offline'
        node.save()
        StorageAgent.real_umount(task, conn)
        conn.close()


    def create_images_pool(self, task):
        node = task.get_obj('Node')

        conn = node.libvirt_conn()
        try:
            pool = conn.storagePoolLookupByName('images')
            if pool.info()[0] != libvirt.VIR_STORAGE_POOL_RUNNING:
                log('Trying to start existing pool', tags=('agent', 'node', 'info'), context=task.logger_ctx)
                pool.build(0)
                pool.create(0)
            else:
                log(msg='Images pool exists', tags=('agent', 'node', 'warning'), context=task.logger_ctx)
        except:
            log(msg='Images pool does not exists. Defining new', tags=('agent', 'node', 'info'), context=task.logger_ctx)
            template = node.images_pool_template()
            pool = conn.storagePoolDefineXML(template, 0)

            try:
                pool.build(0)
            except Exception as e:
                conn.close()
                raise TaskFatalError('node_images_pool_build_failed', exception=e)

            try:
                pool.create(0)
            except Exception as e:
                conn.close()
                raise TaskFatalError('node_images_pool_failed', exception=e)
        conn.close()


    def check(self, task):
        node = task.get_obj('Node')
        conn = node.libvirt_conn()

        for vm in node.vm_set.filter(state__in=['running', 'starting']):
            try:
                libvirt_vm = conn.lookupByName(vm.libvirt_name)
            except Exception as e:
                vm.set_state('stopped')
                vm.save()
                log(msg='Failed to find VM %s at node %s' % (vm.id, vm.node.address), exception=e, tags=('agent', 'node', 'error'), context=task.logger_ctx)

            if libvirt_vm.state()[0] == libvirt.VIR_DOMAIN_RUNNING:
                vm.set_state('running')
                vm.save()
            else:
                vm.set_state('stopped')
                vm.save()
        conn.close()

        node.state = 'ok'
        node.save()


    def suspend(self, task):
        """
        Suspend node to RAM for defined in config seconds. After this time + NODE_WAKEUP_TIME
        node is suspended again, unles it's state is not wake up. Available only
        in admin site or through plugins.
        """
        node = task.get_obj('Node')

        if VM.objects.filter(node=node).exclude(state='closed').count() > 0:
            task.comment = "Node is in use. Aborting suspend"
            task.save()
            return

        node.set_state('suspend')
        node.save()

        log(msg="suspending node %s" % node.address, tags=('agent', 'node', 'info'), context=task.logger_ctx)
        system.call(['ping', '-c', '1', node.address])

        arp = open('/proc/net/arp', 'r').readlines()
        for line in arp:
            fields = line.split()
            if fields[0] == node.address:
                node.set_prop('mac', fields[3])

        node.save()

        conn = node.libvirt_conn()
        conn.suspendForDuration(libvirt.VIR_NODE_SUSPEND_TARGET_MEM, config.get('core', 'NODE_SUSPEND_DURATION'))
        conn.close()


    def wake_up(self, task):
        node = task.get_obj('Node')
        if node.has_prop('mac'):
            system.call(['wakeonlan', node.get_prop('mac')])
            if node.in_state('suspend'):
                time.sleep(config.get('core', 'NODE_WAKEUP_TIME'))
                node.start()
        else:
            raise TaskError('Cannot find node\'s MAC')
