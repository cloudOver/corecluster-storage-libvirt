all:
	echo Nothing to compile

install:
	python setup.py install --root=$(DESTDIR)

egg:
	python setup.py sdist bdist_egg

egg_install:
	python setup.py install

egg_upload:
	# python setup.py sdist bdist_egg upload
	python setup.py sdist upload
egg_clean:
	rm -rf build/ dist/ pyCore.egg-info/ corecluster-storage-libvirt.egg-info/
