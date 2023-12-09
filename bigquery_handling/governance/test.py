import csv 
with open("/Users/gopalkrishna/testing7.csv") as src:
    src_data = csv.reader(src)
    src_data = [item[0] for i, item in enumerate(src_data) if i != 0]
    

with open("/Users/gopalkrishna/Downloads/dt.csv") as src:
    bq_data = csv.reader(src)
    bq_data = [item[0] for i, item in enumerate(bq_data) if i != 0 ]


for item in src_data:
    if item not in bq_data:
        print(item)

exit()
import json 
dt_q = ""
src_q = ""
track = {}
mapping = {
"clasevehiculo_veh":"idClaseVehiculo",
"cliente_com":"idCliente",
"conductor_veh":"idConductor",
"diagramaestados_env":"idDiagramaEstado",
"enviodinamico_env":"idEnvioDinamico",
"envioformapago_env":"idEnvioFormaPago",
"estadopais_cor":"idEstadoPais",
"formaentrega_com":"idFormaEntrega",
"manifiestoenvio_des":"idManifiestoEnvio",
"oficina_org":"idOficina",
"pais_cor":"idPais",
"poblacion_cor":"idPoblacion",
"ruta_des":"idRuta",
"siniestro_env":"idSiniestro",
"tipodocumento_cor":"idTipoDocumento",
"tiponovedadenvio_env":"idTipoNovedad",
"tiponovedadsiniestro_env":"idTipoNovedad",
"tipovehiculo_veh":"idTipoVehiculo",
"transportadorpoblacion_des":"idTransportadorPob",
"usuariorol_cor":"idUsuario",
"estadoenvio_env":"idEstadoEnvio",
"novedadenvio_env":"idNovedadEnvio",
"tiposolucionnovedad_env":"idTipoSolucionNovedad",
"mensajero_org":"idDemografico",
"mensajero_org":"idOficina",
"operador_env":"idOperador",
"docoperacionenvio_int":"idDocOperacionEnvio",
"tipomanifiesto_des":"idTipoManifiesto",
"planilladevolucion_env":"idPlanillaDevolucion",
"repartoenvio_env":"idRepartoEnvio",
"manifiesto_des":"idManifiesto",
"vehiculo_veh":"idVehiculo",
"enviolog_env":"idEnvioLog",
"envioconsolidado_env":"idEnvioConsolidado",
"envionovedadlog_env":"idEnvioNovedadLog",
"operadorpoblacion_env":"idOperadorPob",
"usuario_cor":"idUsuario",
"planillareparto_env":"idPlanillaReparto",
"documentooperacion_int":"idDocumentoOperacion",
"envio_env":"idEnvio",
"transportador_des":"idTransportador",
"demografico_cor":"idDemografico"
}

tbls = {
        "clasevehiculo_veh"
        "cliente_com",
        "conductor_veh",
        "demografico_cor",
        "diagramaestados_env",
        "envionovedadlog_env",
        "estadoenvio_env",
        "estadopais_cor",
        "formaentrega_com",
        "manifiesto_des",
        "manifiestoenvio_des",
        "mensajero_org",
        "novedadenvio_env",
        "oficina_org",
        "pais_cor",
        "planilladevolucion_env",
        "planillareparto_env",
        "poblacion_cor",
        "ruta_des",
        "siniestro_env",
        "tipodocumento_cor",
        "tipomanifiesto_des",
        "tiponovedadenvio_env",
        "tiponovedadsiniestro_env",
        "tiposolucionnovedad_env",
        "transportador_des",
        "transportadorpoblacion_des",
        "usuario_cor",
        "usuariorol_cor",
        "vehiculo_veh",

}
with open("/Users/gopalkrishna/data_governance_automation/test.json") as f:
    reader = json.loads(f.read())
    for item in reader:

        if item['table_name'].replace("btd_scha_corp_pe_tms_", "") not in tbls:
                
            continue
        
        if item['table_name'] not in track:
            track.update({item['table_name']:{}})
            
        if item['column_name'] not in ['dfl_crte_tmst', "dfl_job_name", "dfl_run_id", "dfl_src_name"]:                
            track[item['table_name']].update({item['description']:item['column_name']})
            

src_q = ""
bq_q = ""
for t in track:
    src_q += "SELECT '{0}' as table_name , ".format(t.replace("btd_scha_corp_pe_tms_", ""))
    bq_q += "SELECT '{0}' as table_name , ".format(t)
        
    bq_pt = track[t][mapping[t.replace('btd_scha_corp_pe_tms_', "")]]
    src_pt = mapping[t.replace("btd_scha_corp_pe_tms_", "")] 

    

  
    src_q += " count({0}) as cnt , ".format(src_pt)
    bq_q += " count(distinct {0}) as cnt , ".format(bq_pt)
       
    if "creacion" in track[t]:
        #src_q += " 'creacion' as filter_column , date(creacion) as dt FROM {0} where date(creacion) between '@start_date' and '@end_date' GROUP BY dt \n UNION ALL \n ".format(t.replace("btd_scha_corp_pe_tms_", ""))
        src_q += " 'creacion' as filter_column, min(creacion) min_dt  FROM {0} where date(creacion) <= '@end_date' \n UNION ALL \n".format(t.replace("btd_scha_corp_pe_tms_", ""))
        #bq_q += " '{dt}' as filter_column , date({dt}) as dt FROM tc-sc-bi-bigdata-dfl-qa.trf_corp_pe_dfl_qa.{tbl} where date({dt}) between '@start_date' and '@end_date' GROUP BY dt \n UNION ALL \n ".format(dt=track[t]['creacion'], tbl=t)
        bq_q += " '{dt}' as filter_column, min(creation) as min_dt  FROM tc-sc-bi-bigdata-dfl-qa.trf_corp_pe_dfl_qa.{tbl}  where date(creation) <= '@end_date' \n UNION ALL \n ".format(dt=track[t]['creacion'], tbl=t)
        continue
    if "modificacion" in track[t]:
        #src_q += " 'modificacion' as filter_column , date(modificacion) as dt FROM {0} where date(modificacion) between '@start_date' and '@end_date' GROUP BY dt \n UNION ALL \n ".format(t.replace("btd_scha_corp_pe_tms_", ""))
        src_q += " 'modificacion' as filter_column, min(modificacion) min_dt   FROM {0} where date(modificacion) <= '@end_date' \n UNION ALL \n ".format(t.replace("btd_scha_corp_pe_tms_", ""))
        #bq_q += " '{dt}' as filter_column , date({dt}) as dt FROM tc-sc-bi-bigdata-dfl-qa.trf_corp_pe_dfl_qa.{tbl} where date({dt}) between '@start_date' and '@end_date' GROUP BY dt \n UNION ALL \n ".format(dt=track[t]['modificacion'], tbl=t)
        bq_q += " '{dt}' as filter_column, min(modification) as min_dt  FROM tc-sc-bi-bigdata-dfl-qa.trf_corp_pe_dfl_qa.{tbl} where date(modification) <= '@end_date' \n UNION ALL \n ".format(dt=track[t]['modificacion'], tbl=t)
        continue




with open("/Users/gopalkrishna/data_governance_automation/src.txt", "w") as f:
    f.write(src_q)

with open("/Users/gopalkrishna/data_governance_automation/bq.txt", "w") as f:
    f.write(bq_q)
        