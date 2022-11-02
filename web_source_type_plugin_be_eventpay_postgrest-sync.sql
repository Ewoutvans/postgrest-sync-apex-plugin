prompt --application/set_environment
set define off verify off feedback off
whenever sqlerror exit sql.sqlcode rollback
--------------------------------------------------------------------------------
--
-- Oracle APEX export file
--
-- You should run the script connected to SQL*Plus as the Oracle user
-- APEX_220100 or as the owner (parsing schema) of the application.
--
-- NOTE: Calls to apex_application_install override the defaults below.
--
--------------------------------------------------------------------------------
begin
wwv_flow_imp.import_begin (
 p_version_yyyy_mm_dd=>'2022.04.12'
,p_release=>'22.1.6'
,p_default_workspace_id=>3401859831952937
,p_default_application_id=>101
,p_default_id_offset=>0
,p_default_owner=>'EVENTPAY'
);
end;
/
 
prompt APPLICATION 101 - DataIngress
--
-- Application Export:
--   Application:     101
--   Name:            DataIngress
--   Date and Time:   18:10 Wednesday November 2, 2022
--   Exported By:     EWOUT
--   Flashback:       0
--   Export Type:     Component Export
--   Manifest
--     PLUGIN: 4100290269468286
--   Manifest End
--   Version:         22.1.6
--   Instance ID:     900155337767501
--

begin
  -- replace components
  wwv_flow_imp.g_mode := 'REPLACE';
end;
/
prompt --application/shared_components/plugins/web_source_type/be_eventpay_postgrest_sync
begin
wwv_flow_imp_shared.create_plugin(
 p_id=>wwv_flow_imp.id(4100290269468286)
,p_plugin_type=>'WEB SOURCE TYPE'
,p_name=>'BE.EVENTPAY.POSTGREST-SYNC'
,p_display_name=>'postgrest-sync'
,p_supported_ui_types=>'DESKTOP'
,p_api_version=>2
,p_wsm_capabilities_function=>'ep_postgrest_rest_plugin.capabilities_postgrest'
,p_wsm_fetch_function=>'ep_postgrest_rest_plugin.fetch_postgrest'
,p_wsm_dml_function=>'ep_postgrest_rest_plugin.dml_postgrest'
,p_wsm_discover_function=>'ep_postgrest_rest_plugin.discover_postgrest'
,p_substitute_attributes=>true
,p_subscribe_plugin_settings=>true
,p_version_identifier=>'1.0'
);
end;
/
prompt --application/end_environment
begin
wwv_flow_imp.import_end(p_auto_install_sup_obj => nvl(wwv_flow_application_install.get_auto_install_sup_obj, false));
commit;
end;
/
set verify on feedback on define on
prompt  ...done
