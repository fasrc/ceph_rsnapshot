




  package_data={
        'ceph_rsnapshot': ['templates/*.*'],
    },
    
    
    entry_points={
        'console_scripts': [
            'ceph_rsnapshot=cli:main',
            'export_qcow=cli:export_qcow',
        ],
    },