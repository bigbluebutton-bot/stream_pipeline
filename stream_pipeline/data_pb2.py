# -*- coding: utf-8 -*-
# Generated by the protocol buffer compiler.  DO NOT EDIT!
# source: data.proto
# Protobuf Python Version: 5.26.1
"""Generated protocol buffer code."""
from google.protobuf import descriptor as _descriptor
from google.protobuf import descriptor_pool as _descriptor_pool
from google.protobuf import symbol_database as _symbol_database
from google.protobuf.internal import builder as _builder
# @@protoc_insertion_point(imports)

_sym_db = _symbol_database.Default()




DESCRIPTOR = _descriptor_pool.Default().AddSerializedFile(b'\n\ndata.proto\x12\x04\x64\x61ta\"\xbe\x04\n\x05\x45rror\x12\n\n\x02id\x18\x01 \x01(\t\x12\x0c\n\x04type\x18\x02 \x01(\t\x12\x0f\n\x07message\x18\x03 \x01(\t\x12\x11\n\ttraceback\x18\x04 \x03(\t\x12\x0e\n\x06thread\x18\x05 \x01(\t\x12\x15\n\rstart_context\x18\x06 \x01(\t\x12\x11\n\tthread_id\x18\x07 \x01(\x03\x12\x11\n\tis_daemon\x18\x08 \x01(\x08\x12.\n\nlocal_vars\x18\t \x03(\x0b\x32\x1a.data.Error.LocalVarsEntry\x12\x30\n\x0bglobal_vars\x18\n \x03(\x0b\x32\x1b.data.Error.GlobalVarsEntry\x12:\n\x10\x65nvironment_vars\x18\x0b \x03(\x0b\x32 .data.Error.EnvironmentVarsEntry\x12\x38\n\x0fmodule_versions\x18\x0c \x03(\x0b\x32\x1f.data.Error.ModuleVersionsEntry\x1a\x30\n\x0eLocalVarsEntry\x12\x0b\n\x03key\x18\x01 \x01(\t\x12\r\n\x05value\x18\x02 \x01(\t:\x02\x38\x01\x1a\x31\n\x0fGlobalVarsEntry\x12\x0b\n\x03key\x18\x01 \x01(\t\x12\r\n\x05value\x18\x02 \x01(\t:\x02\x38\x01\x1a\x36\n\x14\x45nvironmentVarsEntry\x12\x0b\n\x03key\x18\x01 \x01(\t\x12\r\n\x05value\x18\x02 \x01(\t:\x02\x38\x01\x1a\x35\n\x13ModuleVersionsEntry\x12\x0b\n\x03key\x18\x01 \x01(\t\x12\r\n\x05value\x18\x02 \x01(\t:\x02\x38\x01\"\x94\x02\n\x11\x44\x61taPackageModule\x12\n\n\x02id\x18\x01 \x01(\t\x12\x11\n\tmodule_id\x18\x02 \x01(\t\x12\x13\n\x0bmodule_name\x18\x03 \x01(\t\x12\x0f\n\x07running\x18\x04 \x01(\x08\x12\x12\n\nstart_time\x18\x05 \x01(\x01\x12\x10\n\x08\x65nd_time\x18\x06 \x01(\x01\x12\x14\n\x0cwaiting_time\x18\x07 \x01(\x01\x12\x12\n\ntotal_time\x18\x08 \x01(\x01\x12,\n\x0bsub_modules\x18\t \x03(\x0b\x32\x17.data.DataPackageModule\x12\x0f\n\x07message\x18\n \x01(\t\x12\x0f\n\x07success\x18\x0b \x01(\x08\x12\x1a\n\x05\x65rror\x18\x0c \x01(\x0b\x32\x0b.data.Error\"\xb9\x01\n\x10\x44\x61taPackagePhase\x12\n\n\x02id\x18\x01 \x01(\t\x12\x10\n\x08phase_id\x18\x02 \x01(\t\x12\x12\n\nphase_name\x18\x03 \x01(\t\x12\x0f\n\x07running\x18\x04 \x01(\x08\x12\x12\n\nstart_time\x18\x05 \x01(\x01\x12\x10\n\x08\x65nd_time\x18\x06 \x01(\x01\x12\x12\n\ntotal_time\x18\x07 \x01(\x01\x12(\n\x07modules\x18\x08 \x03(\x0b\x32\x17.data.DataPackageModule\"\x94\x02\n\x15\x44\x61taPackageController\x12\n\n\x02id\x18\x01 \x01(\t\x12\x15\n\rcontroller_id\x18\x02 \x01(\t\x12\x17\n\x0f\x63ontroller_name\x18\x03 \x01(\t\x12\x0c\n\x04mode\x18\x04 \x01(\t\x12\x0f\n\x07workers\x18\x05 \x01(\x05\x12\x17\n\x0fsequence_number\x18\x06 \x01(\x05\x12\x0f\n\x07running\x18\x07 \x01(\x08\x12\x12\n\nstart_time\x18\x08 \x01(\x01\x12\x10\n\x08\x65nd_time\x18\t \x01(\x01\x12\x14\n\x0cwaiting_time\x18\n \x01(\x01\x12\x12\n\ntotal_time\x18\x0b \x01(\x01\x12&\n\x06phases\x18\x0c \x03(\x0b\x32\x16.data.DataPackagePhase\"\x9c\x02\n\x0b\x44\x61taPackage\x12\n\n\x02id\x18\x01 \x01(\t\x12\x13\n\x0bpipeline_id\x18\x02 \x01(\t\x12\x15\n\rpipeline_name\x18\x03 \x01(\t\x12\x1c\n\x14pipeline_instance_id\x18\x04 \x01(\t\x12\x30\n\x0b\x63ontrollers\x18\x05 \x03(\x0b\x32\x1b.data.DataPackageController\x12\x0c\n\x04\x64\x61ta\x18\x06 \x01(\x0c\x12\x0f\n\x07running\x18\x07 \x01(\x08\x12\x12\n\nstart_time\x18\x08 \x01(\x01\x12\x10\n\x08\x65nd_time\x18\t \x01(\x01\x12\x12\n\ntotal_time\x18\n \x01(\x01\x12\x0f\n\x07success\x18\x0b \x01(\x08\x12\x1b\n\x06\x65rrors\x18\x0c \x03(\x0b\x32\x0b.data.Error\"\x97\x01\n\tRequestDP\x12\'\n\x0c\x64\x61ta_package\x18\x01 \x01(\x0b\x32\x11.data.DataPackage\x12\"\n\x1a\x64\x61ta_package_controller_id\x18\x02 \x01(\t\x12\x1d\n\x15\x64\x61ta_package_phase_id\x18\x03 \x01(\t\x12\x1e\n\x16\x64\x61ta_package_module_id\x18\x04 \x01(\t\"W\n\x10ReturnDPandError\x12\'\n\x0c\x64\x61ta_package\x18\x01 \x01(\x0b\x32\x11.data.DataPackage\x12\x1a\n\x05\x65rror\x18\x02 \x01(\x0b\x32\x0b.data.Error2?\n\rModuleService\x12.\n\x03run\x12\x0f.data.RequestDP\x1a\x16.data.ReturnDPandErrorb\x06proto3')

_globals = globals()
_builder.BuildMessageAndEnumDescriptors(DESCRIPTOR, _globals)
_builder.BuildTopDescriptorsAndMessages(DESCRIPTOR, 'data_pb2', _globals)
if not _descriptor._USE_C_DESCRIPTORS:
  DESCRIPTOR._loaded_options = None
  _globals['_ERROR_LOCALVARSENTRY']._loaded_options = None
  _globals['_ERROR_LOCALVARSENTRY']._serialized_options = b'8\001'
  _globals['_ERROR_GLOBALVARSENTRY']._loaded_options = None
  _globals['_ERROR_GLOBALVARSENTRY']._serialized_options = b'8\001'
  _globals['_ERROR_ENVIRONMENTVARSENTRY']._loaded_options = None
  _globals['_ERROR_ENVIRONMENTVARSENTRY']._serialized_options = b'8\001'
  _globals['_ERROR_MODULEVERSIONSENTRY']._loaded_options = None
  _globals['_ERROR_MODULEVERSIONSENTRY']._serialized_options = b'8\001'
  _globals['_ERROR']._serialized_start=21
  _globals['_ERROR']._serialized_end=595
  _globals['_ERROR_LOCALVARSENTRY']._serialized_start=385
  _globals['_ERROR_LOCALVARSENTRY']._serialized_end=433
  _globals['_ERROR_GLOBALVARSENTRY']._serialized_start=435
  _globals['_ERROR_GLOBALVARSENTRY']._serialized_end=484
  _globals['_ERROR_ENVIRONMENTVARSENTRY']._serialized_start=486
  _globals['_ERROR_ENVIRONMENTVARSENTRY']._serialized_end=540
  _globals['_ERROR_MODULEVERSIONSENTRY']._serialized_start=542
  _globals['_ERROR_MODULEVERSIONSENTRY']._serialized_end=595
  _globals['_DATAPACKAGEMODULE']._serialized_start=598
  _globals['_DATAPACKAGEMODULE']._serialized_end=874
  _globals['_DATAPACKAGEPHASE']._serialized_start=877
  _globals['_DATAPACKAGEPHASE']._serialized_end=1062
  _globals['_DATAPACKAGECONTROLLER']._serialized_start=1065
  _globals['_DATAPACKAGECONTROLLER']._serialized_end=1341
  _globals['_DATAPACKAGE']._serialized_start=1344
  _globals['_DATAPACKAGE']._serialized_end=1628
  _globals['_REQUESTDP']._serialized_start=1631
  _globals['_REQUESTDP']._serialized_end=1782
  _globals['_RETURNDPANDERROR']._serialized_start=1784
  _globals['_RETURNDPANDERROR']._serialized_end=1871
  _globals['_MODULESERVICE']._serialized_start=1873
  _globals['_MODULESERVICE']._serialized_end=1936
# @@protoc_insertion_point(module_scope)
