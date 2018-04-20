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


import base64
import libvirt
import urllib
from corecluster.agents.base_agent import BaseAgent
from corecluster.models.core import Device
from corecluster.exceptions.agent import *
from corecluster.cache.data_chunk import DataChunk
from corenetwork.utils import system
from corenetwork.utils.logger import log


class AgentThread(BaseAgent):
    task_type = 'image'
    supported_actions = ['create', 'upload_url', 'upload_data', 'delete', 'attach', 'detach']
    lock_on_fail = ['create', 'upload_url', 'upload_data', 'delete', 'duplicate']

    def task_failed(self, task, exception):
        if task.action in self.lock_on_fail:
            image = task.get_obj('Image')
            image.set_state('failed')
            image.save()

        super(AgentThread, self).task_failed(task, exception)


    def get_storage(self, image, conn):
        if not image.storage.in_state('ok'):
            raise TaskError('storage_unavailable')

        try:
            storage = conn.storagePoolLookupByName(image.storage.name)
        except Exception as e:
            image.storage.set_state('locked')
            image.storage.save()
            raise TaskFatalError('libvirt_storage_not_found', exception=e)

        if storage.info()[0] != libvirt.VIR_STORAGE_POOL_RUNNING:
            image.storage.set_state('locked')
            image.storage.save()
            raise TaskError('libvrit_storage_not_running')

        storage.refresh(0)
        return storage


    def create(self, task):
        image = task.get_obj('Image')
        conn = libvirt.open('qemu:///system')
        storage = self.get_storage(image, conn)

        volume_xml = image.libvirt_xml()

        try:
            volume = storage.createXML(volume_xml, 0)
        except Exception as e:
            raise TaskError('cannot_create_image', exception=e)

        storage.refresh(0)

        image.size = volume.info()[1]
        image.set_state('ok')
        image.save()

        conn.close()


    def upload_url(self, task):
        '''
        Download datq from url and put its contents into given image. Operation.data
        should contains:
        - action
        - url
        - size
        '''
        image = task.get_obj('Image')
        if image.attached_to != None:
            raise TaskError('image_attached')

        image.set_state('downloading')
        image.save()

        conn = libvirt.open('qemu:///system')
        storage = self.get_storage(image, conn)
        storage.refresh(0)

        try:
            volume = storage.storageVolLookupByName(image.libvirt_name)
        except Exception as e:
            raise TaskFatalError('libvirt_image_not_found', exception=e)

        try:
            remote = urllib.urlopen(task.get_prop('url'))
        except Exception as e:
            raise TaskError('url_not_found', exception=e)

        bytes = 0
        while bytes < int(task.get_prop('size')):
            data = remote.read(1024*250)
            if len(data) == 0:
                break
            stream = conn.newStream(0)
            volume.upload(stream, bytes, len(data), 0)
            stream.send(data)
            stream.finish()
            bytes += len(data)

            image = task.get_obj('Image')
            image.set_prop('progress', float(bytes)/float(task.get_prop('size')))
            image.save()

        remote.close()

        log(msg="Rebasing image to no backend", tags=('agent', 'image', 'info'), context=task.logger_ctx)
        if image.format in ['qcow2', 'qed']:
            r = system.call(['sudo',
                             'qemu-img', 'rebase',
                             '-u',
                             '-f', image.format,
                             '-u',
                             '-b', '',
                             volume.path()], stderr=None, stdout=None)
            if r != 0:
                image = task.get_obj('Image')
                image.set_state('failed')
                image.save()
                conn.close()
                return

        storage.refresh(0)
        image = task.get_obj('Image')
        image.size = volume.info()[1]
        image.set_state('ok')
        image.save()
        conn.close()


    def upload_data(self, task):
        '''
        Put file given in operation.data['filename'] into given image (operation.image)
        at offset. The file can extend existing image. Operation.data should contain:
        - action
        - offset
        - filename
        '''
        image = task.get_obj('Image')
        if image.attached_to != None:
            raise TaskError('image_attached')

        image.set_state('downloading')
        image.save()

        conn = libvirt.open('qemu:///system')
        storage = self.get_storage(image, conn)
        storage.refresh(0)

        try:
            volume = storage.storageVolLookupByName(image.libvirt_name)
        except Exception as e:
            raise TaskFatalError('libvirt_image_not_found', exception=e)

        data_chunk = DataChunk(cache_key=task.get_prop('chunk_id'))
        data = base64.b64decode(data_chunk.data)

        stream = conn.newStream(0)
        volume.upload(stream, int(data_chunk.offset), len(data), 0)
        stream.send(data)
        stream.finish()

        data_chunk.delete()

        log(msg="Rebasing image to no backend", tags=('agent', 'image', 'info'), context=task.logger_ctx)
        if image.format in ['qcow2', 'qed']:
            r = system.call(['sudo',
                             'qemu-img', 'rebase',
                             '-u',
                             '-f', image.format,
                             '-u',
                             '-b', '',
                             volume.path()], stderr=None, stdout=None)
            if r != 0:
                image = task.get_obj('Image')
                image.set_state('failed')
                image.save()
                conn.close()
                return

        storage.refresh(0)
        image = task.get_obj('Image')
        image.size = volume.info()[1]
        image.set_state('ok')
        image.save()

        conn.close()


    def delete(self, task):
        image = task.get_obj('Image')
        if image.attached_to != None and not image.attached_to.in_state('closed') and not task.ignore_errors:
            raise TaskError('image_attached')

        for vm in image.vm_set.all():
            if not vm.in_state('closed'):
                task.ignore_errors = True
                raise TaskError('image_attached')

        conn = libvirt.open('qemu:///system')
        storage = self.get_storage(image, conn)

        try:
            volume = storage.storageVolLookupByName(image.libvirt_name)
            volume.delete(0)
        except Exception as e:
            log(msg='Image doesn\'t exists. Skipping', exception=e, tags=('agent', 'image', 'error'), context=task.logger_ctx)

        image = task.get_obj('Image')
        image.set_state('deleted')
        image.save()

        conn.close()


    def attach(self, task):
        vm = task.get_obj('VM')

        vm.node.check_online(task.ignore_errors)

        image = task.get_obj('Image')
        conn = vm.node.libvirt_conn()

        storage = conn.storagePoolLookupByName(image.storage.name)
        storage.refresh(0)

        if image.attached_to != None and not image.attached_to.in_state('closed'):
            raise TaskError('image_attached')

        if not vm.in_state('stopped'):
            raise TaskError('vm_not_stopped')

        if not image.in_state('ok'):
            raise TaskError('image_state')

        devices = [i.disk_dev for i in vm.image_set.all()]
        if 'device' in task.get_all_props().keys() and not int(task.get_prop('device')) in devices:
            disk_dev = int(task.get_prop('device'))
        else:
            disk_dev = 1
            while disk_dev in devices:
                disk_dev = disk_dev+1

        image = task.get_obj('Image')
        image.disk_dev = disk_dev
        image.attached_to = vm
        image.save()

        Device.create(image.id, vm, 'devices/image.xml', {'img': image, 'disk_dev': 'sd' + chr(ord('a')+disk_dev)})

        vm.libvirt_redefine()

        conn.close()


    def detach(self, task):
        vm = task.get_obj('VM')

        vm.node.check_online(task.ignore_errors)

        image = task.get_obj('Image')

        conn = vm.node.libvirt_conn()
        if not vm.in_states(['stopped', 'closing', 'closed']) and not task.ignore_errors:
            raise TaskError('vm_not_stopped')

        image.attached_to = None
        image.save()

        for device in Device.objects.filter(object_id=image.id).all():
            device.delete()

        try:
            vm.libvirt_redefine()
        except:
            pass

        conn.close()
