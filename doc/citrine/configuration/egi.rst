.. highlight:: rst

.. index::
   single: EGI scripts

EGI scripts
============

EOS provides a couple of scripts that generate the required space accounting information [https://wiki.egi.eu/wiki/APEL/Storage] and info provider data. These scrips are available in the `eos-server` package starting with release 5.0.15.

Storage accounting
------------------

This information is provided by the `eos-star-accounting.py` script that looks at the EOS space configuration present in your instance. An example of how to invoke this script is provided below with some demo information:

.. code-block:: bash

   eos-star-accounting.py
   <sr:StorageUsageRecords xmlns:sr="http://eu-emi.eu/namespaces/2011/02/storagerecord">
     <sr:StorageUsageRecord>
       <sr:RecordIdentity sr:createTime="2022-03-21T14:22:21Z" sr:recordId="esdss000.cern.ch-52307ef4-a922-11ec-bc51-dc4a3e6b9f27"/>
       <sr:StorageSystem>esdss000.cern.ch</sr:StorageSystem>
       <sr:SubjectIdentity>
         <sr:Site>eosdev</sr:Site>
       </sr:SubjectIdentity>
       <sr:StorageMedia>disk</sr:StorageMedia>
       <sr:StartTime>2022-03-20T14:22:21Z</sr:StartTime>
       <sr:EndTime>2022-03-21T14:22:21Z</sr:EndTime>
       <sr:FileCount>1289</sr:FileCount>
       <sr:ResourceCapacityUsed>1287017889792</sr:ResourceCapacityUsed>
       <sr:ResourceCapacityAllocated>1287017889792</sr:ResourceCapacityAllocated>
       <sr:LogicalCapacityUsed>1287017889792</sr:LogicalCapacityUsed>
     </sr:StorageUsageRecord>
   </sr:StorageUsageRecords>


Info provider
--------------

This information is provided by the `eos-info-provider.py` script.

.. code-block:: bash

 eos-info-provider.py --sitename eosdev
 version: 1
 dn: GLUE2ServiceID=esdss000.cern.ch/Service,GLUE2GroupID=resource,o=glue
 changetype: add
 objectClass: GLUE2Service
 objectClass: GLUE2StorageService
 GLUE2ServiceID: esdss000.cern.ch/Service
 GLUE2EntityCreationTime: 2022-03-21T14:24:55Z
 GLUE2ServiceQualityLevel: production
 GLUE2ServiceCapability: data.access.flatfiles
 GLUE2ServiceCapability: data.transfer
 GLUE2ServiceCapability: data.management.replica
 GLUE2ServiceCapability: data.management.storage
 GLUE2ServiceCapability: data.management.transfer
 GLUE2ServiceCapability: security.authentication
 GLUE2ServiceCapability: security.authorization
 GLUE2ServiceType: eos
 GLUE2ServiceAdminDomainForeignKey: eosdev
 version: 1
 dn: GLUE2StorageServiceCapacityID=esdss000.cern.ch/StorageServiceCapacity,GLUE
   2ServiceID=esdss000.cern.ch/Service,GLUE2GroupID=resource,o=glue
 changetype: add
 objectClass: GLUE2StorageServiceCapacity
 GLUE2StorageServiceCapacityUsedSize: 1198
 GLUE2EntityCreationTime: 2022-03-21T14:24:55Z
 GLUE2StorageServiceCapacityType: online
 GLUE2StorageServiceCapacityID: esdss000.cern.ch/StorageServiceCapacity
 GLUE2StorageServiceCapacityFreeSize: 867
 GLUE2StorageServiceCapacityStorageServiceForeignKey: esdss000.cern.ch/Service
 GLUE2StorageServiceCapacityTotalSize: 2065
 version: 1
 dn: GLUE2ManagerID=esdss000.cern.ch/Manager,GLUE2ServiceID=esdss000.cern.ch/Se
   rvice,GLUE2GroupID=resource,o=glue
 changetype: add
 objectClass: GLUE2StorageManager
 objectClass: GLUE2Manager
 GLUE2ManagerProductName: EOS
 GLUE2EntityCreationTime: 2022-03-21T14:24:58Z
 GLUE2ManagerProductVersion:
 GLUE2StorageManagerStorageServiceForeignKey: esdss000.cern.ch/Service
 GLUE2ManagerServiceForeignKey: esdss000.cern.ch/Service
 GLUE2ManagerID: esdss000.cern.ch/Manager
 version: 1
 dn: GLUE2ResourceID=esdss000.cern.ch/DataStore,GLUE2ManagerID=esdss000.cern.ch
   /Manager,GLUE2ServiceID=esdss000.cern.ch/Service,GLUE2GroupID=resource,o=glue
 changetype: add
 objectClass: GLUE2DataStore
 GLUE2DataStoreLatency: online
 GLUE2DataStoreFreeSize: 867
 GLUE2ResourceManagerForeignKey: esdss000.cern.ch/Manager
 GLUE2EntityCreationTime: 2022-03-21T14:24:58Z
 GLUE2DataStoreType: disk
 GLUE2DataStoreUsedSize: 1198
 GLUE2DataStoreStorageManagerForeignKey: esdss000.cern.ch/Manager
 GLUE2ResourceID: esdss000.cern.ch/DataStore
 GLUE2DataStoreTotalSize: 2065
