"""
Copyright (C) 2014-2017 cloudover.io ltd.
This file is part of the CloudOver.org project

Licensee holding a valid commercial license for this software may
use it in accordance with the terms of the license agreement
between cloudover.io ltd. and the licensee.

Alternatively you may use this software under following terms of
GNU Affero GPL v3 license:

This program is free software: you can redistribute it and/or modify
it under the terms of the GNU Affero General Public License as
published by the Free Software Foundation, either version 3 of the
License, or (at your option) any later version. For details contact
with the cloudover.io company: https://cloudover.io/


This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU Affero General Public License for more details.


You should have received a copy of the GNU Affero General Public License
along with this program.  If not, see <http://www.gnu.org/licenses/>.
"""


import libvirt
import os

from corecluster.agents.base_agent import BaseAgent
from corecluster.exceptions.agent import *
from corenetwork.utils.logger import log


class AgentThread(BaseAgent):
    storage = None
    task_type = 'storage'
    supported_actions = ['mount', 'umount']


    def task_error(self, task, exception):
        storage = task.get_obj('Storage')
        if storage.state != 'disabled':
            storage.set_state('locked')
        storage.save()
        super(AgentThread, self).task_error(task, exception)


    def task_finished(self, task):
        storage = task.get_obj('Storage')
        storage.save()
        super(AgentThread, self).task_finished(task)


    def task_failed(self, task, exception):
        storage = task.get_obj('Storage')
        storage.save()
        super(AgentThread, self).task_failed(task, exception)


    def mount(self, task):
        conn = libvirt.open('qemu:///system')
        AgentThread.real_mount(task, conn)
        conn.close()


    @staticmethod
    def real_mount(task, conn):
        """
        This function mounts storage by connection given in parameter. This could be used both, by Core (Storage thread)
        and Node thread for mounting nodes.
        """
        storage = task.get_obj('Storage')
        if storage.state == 'disabled':
            raise TaskError('storage_disabled')

        storage.set_state('locked')
        storage.save()

        #TODO: Do it better, unless Libvirt doesnt create new directory via storage.build
        if storage.transport == 'netfs':
            try:
                os.mkdir('/var/lib/cloudOver/storages/' + storage.name)
            except:
                log(msg='Failed to create storages directory. Going ahead', tags=('storage', 'agent', 'info'), context=task.logger_ctx)

        try:
            lv_storage = conn.storagePoolLookupByName(storage.name)
            if lv_storage.info()[0] == libvirt.VIR_STORAGE_POOL_RUNNING:
                lv_storage.setAutostart(False)
                storage.state = 'ok'
                storage.save()
                return

            try:
                lv_storage.destroy()
            except:
                pass

            lv_storage.undefine()
        except Exception as e:
            log(msg='removing storage failed. Probably storage doesn\'t exist. Going ahead',
                exception=e,
                tags=('storage', 'agent', 'info'),
                context=task.logger_ctx)

        storage_xml = storage.libvirt_template()
        try:
            conn.storagePoolDefineXML(storage_xml, 0)
        except Exception as e:
            log(msg='Storage define failed. Going ahead', exception=e, tags=('info', 'agent', 'storage'))

        pool = conn.storagePoolLookupByName(storage.name)
        pool.setAutostart(False)
        try:
            pool.build(0)
        except Exception as e:
            log(msg='storage build failed. Going ahead',
                exception=e,
                tags=('storage', 'agent', 'info'),
                context=task.logger_ctx)

        try:
            pool.create(0)
        except Exception as e:
            storage.set_state('locked')
            storage.save()
            log(msg='Storage create failed',
                exception=e,
                tags=('storage', 'agent', 'error'),
                context=task.logger_ctx)
            raise TaskFatalError('storage_create_failed', exception=e)

        storage.set_state('ok')
        storage.save()


    def umount(self, operation):
        conn = libvirt.open('qemu:///system')
        AgentThread.real_mount(operation, conn)

        operation.storage.state = 'locked'
        operation.storage.save()
        conn.close()


    @staticmethod
    def real_umount(task, conn):
        """
        As above, this function mounts storage by connection. This could be used by Storage or Node agents
        """
        storage = task.get_obj('Storage')

        try:
            lv_storage = conn.storagePoolLookupByName(storage.name)
        except Exception as e:
            storage.state = 'locked'
            storage.save()
            return

        try:
            try:
                lv_storage.destroy()
                lv_storage.undefine()
            except:
                lv_storage.undefine()
        except Exception as e:
            storage.state = 'locked'
            storage.save()
            conn.close()
            raise TaskError('storage_undefine', exception=e)
