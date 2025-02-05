import sys
import pyodbc
from hdbcli import dbapi
from decouple import config
sys.path.insert(3,'C:/Users/irojas/Desktop/Pruebas/repro_proyecto/repro_api/app/utils')
from app.utils.functions import obj

ADDRESS=config("ADDRESS")
PORT=config("PORT")
USER=config("USER")
PASSWORD=config("PASSWORD")


class dbConnection:
  def __init__(self):
    self.selectArray = []
    pass

  def openDB(self):
    connectionString = f'DRIVER={{SQL Server}};SERVER={'192.168.5.4'};DATABASE={'web_erp_cantol'};UID={'sa'};PWD={'Cantol123@abc'}'
    self.cnxn = pyodbc.connect(connectionString)
    self.cursor = self.cnxn.cursor()

  def closeDB(self): 
    self.cursor.close()
    self.cnxn.close()
    

  def getData(self):
    self.openDB()
    SQL_QUERY = "select top 40000 * from (select T.SubArea, T.SAPNumero, T.CodigoArticulo, T.NombreArticulo, (select SUM(od.cantidad_prod_mala) from oee_cab oc inner join oee_det od on oc.id_oee = od.id_oee where od.num_of_sap = T.SAPNumero) as ProduccionMala, (select SUM(od.cantidad_prod_buena) from oee_cab oc inner join oee_det od on oc.id_oee = od.id_oee where od.num_of_sap = T.SAPNumero) as ProduccionBuena , (select SUM(od.cantidad_total) from oee_cab oc inner join oee_det od on oc.id_oee = od.id_oee where od.num_of_sap = T.SAPNumero) as ProduccionTotal from (select oc.id_oee, oc.fecha_doc as fe, sa.nom_subarea as SubArea, od.cod_articulo as CodigoArticulo, od.articulo as NombreArticulo, od.cantidad_prod_mala as ProduccionMala, od.cantidad_prod_buena as ProduccionBuena, od.cantidad_total as ProduccionTotal, od.causa as Causa, od.motivo_causa as Motivo, od.detalle as Detalle, od.num_of_sap as SAPNumero from oee_cab oc inner join sub_area sa on oc.cod_subarea = sa.cod_subarea inner join oee_det od on oc.id_oee  = od.id_oee where od.cantidad_prod_mala > 0 and od.tipo_registro = 'PRODUCCION') as T GROUP BY T.SAPNumero, T.SubArea, T.CodigoArticulo, T.NombreArticulo) as F"
    self.cursor.execute(SQL_QUERY)
    records = self.cursor.fetchall()
    self.closeDB()
    return records

  def get_motivo_causa_areas(self, saPattern):
    self.selectArray.clear()
    self.openDB()
    SQL_QUERY = f"select cod_motivo_causa, motivo_causa from oee_motivo_causa mc inner join oee_causa c on mc.cod_causa = c.cod_causa inner join sub_area sa on mc.cod_subarea = sa.cod_subarea where c.cod_causa = 11 and UPPER(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(nom_subarea, 'Á', 'A'), 'É', 'E'), 'Í', 'I'), 'Ó', 'O'), 'Ú', 'U')) LIKE UPPER(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE('%{saPattern}%', 'Á', 'A'), 'É', 'E'), 'Í', 'I'), 'Ó', 'O'), 'Ú', 'U')) group by cod_motivo_causa, motivo_causa;"
    self.cursor.execute(SQL_QUERY)
    for row in self.cursor:
        self.selectArray.append(row)
    self.closeDB()
    return self.selectArray
      
  #obtiene id y nombre de subarea
  def getAllSubAreas(self):
    SQL_QUERY = "SELECT sa.cod_subarea , sa.nom_subarea FROM sub_area sa;"
    self.openDB()
    self.cursor.execute(SQL_QUERY)
    records = self.cursor.fetchall()
    self.closeDB()
    return records

  #insertar simulacion
  def postSimulation(self, model, DocNumSAP, isChecked = False):
    self.openDB()
    try:
      SQL_QUERY = f"INSERT INTO reproceso_cab ({"DocNumSAP, " if isChecked else ""}ItemCode, ProdName, Type, PlannedQty, SimDate, PostDate, DueDate, StartDate,Warehouse, OriginType, prcCode, Usuario_Codigo_SIM, Usuario_Codigo_VAL, cod_motivo_causa, isChecked) VALUES ({("'"+ DocNumSAP + "', ") if isChecked else ""}'{model.ItemCode}', '{model.ProdName}','{model.Type}',{model.PlannedQty}, '{model.SimDate}', '{model.PostDate}','{model.DueDate}','{model.StartDate}','{model.Warehouse}', '{model.OriginType}', '{model.PrcCode}', '{model.Usuario_Codigo_SIM}', {'NULL' if model.Usuario_Codigo_VAL is None else model.Usuario_Codigo_VAL}, {'NULL' if model.cod_motivo_causa is None else model.cod_motivo_causa}, {int(model.isChecked)});"
      stmt = "INSERT INTO reproceso_det (id_cab, ItemCode, ItemName, IssuedType, PlannedQty, DueDate, StartDate, wareHouse, ItemType, OcrCode, OcrCode2, baseCost) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)"
      self.cursor.execute(SQL_QUERY)
      self.cnxn.commit()
      SQL_QUERY_GET_LAST = "SELECT top 1 id_cab FROM reproceso_cab ORDER BY id_cab DESC"
      self.cursor.execute(SQL_QUERY_GET_LAST)
      simulationId = self.cursor.fetchone()[0]
      if len(model.ProductionOrderLines) > 0:
        self.cursor.executemany(stmt, self.get_reproceso_det_format(simulationId, model.ProductionOrderLines))
        self.cnxn.commit()
      self.closeDB()
      return 201, {"SWC_ID" : simulationId}
    except Exception as e:
      self.closeDB()
      print(f"An error ocurred: {e}")
      return 500, {"SWC_ID" : "XXXXXXX"}

 
  def get_reproceso_det_format(self, simulationId, ProductionOrderLines):
    ProductionOrderLines_tuple = [(str(simulationId), x.ItemNo, x.ItemName, x.ProductionOrderIssueType, str(x.PlannedQuantity), x.EndDate, x.StartDate, x.Warehouse, x.ItemType, x.DistributionRule, x.DistributionRule2, str(x.baseCost)) for x in ProductionOrderLines if bool(x)]
    return ProductionOrderLines_tuple


  def get_whole_simulations(self, usuarioid):
    try:
      self.openDB()
      SQL_QUERY_GET_LAST = ("SELECT id_cab, ItemCode, ProdName, PlannedQty, SimDate, PostDate, DueDate, StartDate, WareHouse, Usuario_Codigo_SIM as Usuario_Nombre, motivo_causa, isChecked, DocNumSAP, (select (rd.PlannedQty * rd.baseCost) from reproceso_det rd where id_cab = rc.id_cab and ItemCode = rc.ItemCode) as initialCost, (select sum(rd.PlannedQty * rd.baseCost) from reproceso_det rd where id_cab = rc.id_cab and ItemCode != rc.ItemCode) as additionalCost FROM reproceso_cab rc LEFT JOIN (select * from oee_motivo_causa omc where cod_causa = 11) as omc ON rc.cod_motivo_causa = omc.cod_motivo_causa ORDER BY rc.SimDate DESC, id_cab DESC;" if usuarioid == 'XXXX' else f"SELECT id_cab, ItemCode, ProdName, PlannedQty, SimDate, PostDate, DueDate, StartDate, WareHouse, Usuario_Nombre, motivo_causa, isChecked, DocNumSAP, (select (rd.PlannedQty * rd.baseCost) from reproceso_det rd where id_cab = rc.id_cab and ItemCode = rc.ItemCode) as initialCost, (select sum(rd.PlannedQty * rd.baseCost) from reproceso_det rd where id_cab = rc.id_cab and ItemCode != rc.ItemCode) as additionalCost FROM reproceso_cab rc INNER JOIN sub_area sa ON rc.PrcCode = sa.centro_costo_sap INNER JOIN cali_personal cp ON sa.usuario_login = cp.usuarioid INNER JOIN Usuarios_Cantol.dbo.tUsuarios tu ON rc.Usuario_Codigo_SIM = tu.Usuario_Codigo LEFT JOIN (select * from oee_motivo_causa omc where cod_causa = 11) as omc ON rc.cod_motivo_causa = omc.cod_motivo_causa WHERE cp.usuarioid = '{usuarioid}' AND cp.rol_id = 4 ORDER BY rc.SimDate DESC, id_cab DESC;")
      self.cursor.execute(SQL_QUERY_GET_LAST)
      simulations = self.cursor.fetchall()
      self.closeDB()
      return simulations
    except Exception as error:
      self.closeDB()
      print(f"An error ocurred: {error}")
      return []

  def validateRol(self, usuarioid, rol_id):
    try:
      self.openDB()
      QUERY = f"select 1 from cali_personal cp where usuarioid = '{usuarioid}' and rol_id = {rol_id}"
      self.cursor.execute(QUERY)
      isOk = self.cursor.fetchone()
      self.closeDB()
      return isOk
    except:
      return []

  def get_database_model(self, id_cab):
    try:
      self.openDB()
      QUERY0 = f"SELECT * FROM reproceso_cab WHERE id_cab = {str(id_cab)};"
      self.cursor.execute(QUERY0)
      data_cabecera = self.cursor.fetchall()[0]
      QUERY1 = f"SELECT * FROM reproceso_det WHERE id_cab = {str(id_cab)};"
      self.cursor.execute(QUERY1)
      data_detalle = self.cursor.fetchall()
      self.closeDB()
      return self.get_of_sap_format(data_cabecera,data_detalle)
    except Exception as error:
      print(f"An Error Ocurred on get_database_model: {error}")
      return None, False

  def set_validate_sim(self, usuarioid, id_cab, sap_number):
    try:
      self.openDB()
      QUERY0 = f"UPDATE reproceso_cab SET DocNumSAP = {str(sap_number)}, Usuario_Codigo_VAL = '{usuarioid}', isChecked = 1 WHERE id_cab = {str(id_cab)};"
      self.cursor.execute(QUERY0)
      self.cnxn.commit()
      self.closeDB()
      return True
    except Exception as error:
      print(f"An Error Ocurred on get_database_model: {error}")
      return False

  def get_of_sap_format(self, data_cabecera, data_detalle):
    ProductionOrderLines = [obj({"ItemNo": x[1], "PlannedQuantity": x[4], "ProductionOrderIssueType": x[3], "Warehouse": x[7], "DistributionRule": x[9], "DistributionRule2": x[10], "ItemType": x[8], "StartDate": x[6], "EndDate": x[5], "ItemName": x[2]}) for x in data_detalle if bool(x)]
    try:
      data_dict = {
      "ItemCode": data_cabecera[2],
      "ProdName": data_cabecera[3],
      "PlannedQty": data_cabecera[5],
      "PostDate": data_cabecera[7],
      "DueDate": data_cabecera[8],
      "StartDate": data_cabecera[9],
      "Warehouse": data_cabecera[10],
      "Usuario_Codigo_SIM": data_cabecera[13],
      "ProductionOrderLines": ProductionOrderLines,
      }
      OBJsap = obj(data_dict)
      return OBJsap, True
    except Exception as error:
      print(f"An error ocurred: {error}")
      return None, False
      

    

class sapConnection:
    def __init__(self):
        self.selectArray = []
        pass
    
    def openConnection(self):
        self.con = dbapi.connect(address=ADDRESS, port=PORT, user=USER, password=PASSWORD)
        self.cursor = self.con.cursor()
    
    def closeCursor(self):
        self.cursor.close()
        self.con.close()

    # def getDatafromTable(self, tableName : str = None, limit : int = 0):
    def getDataProductionByOF(self, of: str = None):
      if not(not(of)):
          self.selectArray.clear()
          # sql = "SELECT * from {} WHERE \"Father\" = '{}'".format(tableName, productCode)
          sql = "SELECT \"DocNum\", \"ItemCode\", \"PostDate\", \"CloseDate\", \"Warehouse\" FROM SBO_TECNO_PRODUCCION.OWOR WHERE \"DocNum\" = {}".format(of)
          # sql = f'select * from {tableName}' + ' ' + (('limit ' + str(limit)) if limit > 0 else '')
          self.openConnection()
          self.cursor.execute(sql)
          record = self.cursor.fetchall()
          return record
      else:
        print("invalid input parameters")

    # def getDatafromTable(self, tableName : str = None, limit : int = 0):
    def getFailsOF(self):
      self.selectArray.clear()
      sql = "SELECT top 50 * FROM (SELECT ow.\"DocNum\" AS of, ow.\"ItemCode\" AS productId, itm.\"ItemName\" AS productName, ow.\"RjctQty\", ow.\"CmpltQty\", ow.\"PlannedQty\", ow.\"PostDate\", ow.\"CloseDate\", ow.\"Warehouse\" FROM SBO_TECNO_PRODUCCION.OWOR ow INNER JOIN SBO_TECNO_PRODUCCION.OITM itm ON ow.\"ItemCode\" = itm.\"ItemCode\" WHERE RIGHT(ow.\"ItemCode\", 1) !='R' AND ow.\"RjctQty\" > 0 ORDER BY ow.\"PostDate\" DESC)"
      self.openConnection()
      self.cursor.execute(sql)
      response = self.cursor.fetchall()
      self.closeCursor()
      return response
    
    #obtiene lista de materiales por idProduct para reproceso
    # def getMaterialsByAD(self, od: str = None, subarea: str = None):
    def getMaterialsByAD(self, od: str = None):
        # splited_subarea = subarea.split(" ")
        self.selectArray.clear()
        sql = "SELECT '0.0' AS NroOrden, 'ARTICULO' AS TIPO, OTM.\"ItemCode\" AS Codigo_Det, OTM.\"ItemName\" AS Nombre_Det, 'UND' AS Und, 1 AS Cantidad, 1 AS Costo, (SELECT \"LastPurPrc\" FROM SBO_TECNO_PRODUCCION.OITM OIT WHERE OIT.\"ItemCode\" = '{}') AS costo_Uno, 0 AS Stock, 'Undefined' AS WareCod FROM SBO_TECNO_PRODUCCION.OITM AS OTM WHERE \"ItemCode\" = '{}' UNION SELECT concat('1.' , TO_VARCHAR(\"ChildNum\")) as NroOrden, IFNULL(CASE WHEN \"Type\" = '290' THEN 'RECURSO' WHEN \"Type\" = '04' THEN 'ARTICULO' ELSE 'OTROS' END ,'OTROS') AS TIPO, RD.\"Code\" as Codigo_Det, RD.\"ItemName\" as Nombre_Det, It.\"InvntryUom\" AS Und, \"Quantity\" as Cantidad, CASE WHEN RD.\"Code\" LIKE '%GGRC%' THEN It.\"AvgPrice\" ELSE It.\"LastPurPrc\" END as Costo, (SELECT \"LastPurPrc\" FROM SBO_TECNO_PRODUCCION.OITM OIT WHERE OIT.\"ItemCode\" = RD.\"Father\") AS costo_Uno, 0 AS Stock, 'Undefined' AS WareCod FROM SBO_TECNO_PRODUCCION.ITT1 RD LEFT JOIN SBO_TECNO_PRODUCCION.OITM It ON It.\"ItemCode\" = RD.\"Code\" WHERE RD.\"Father\" = '{}' AND RD.\"Code\" NOT LIKE '%MOH%' AND RD.\"Code\" NOT LIKE '%MOM%' UNION SELECT '2.0' AS NROORDEN, IFNULL(CASE WHEN \"ObjType\" = '290' THEN 'RECURSO' WHEN \"ObjType\" = '04' THEN 'ARTICULO' ELSE 'OTROS' END ,'OTROS') AS TIPO, \"ResCode\" AS CODIGO_DET, \"ResName\" AS NOMBRE_DET, \"UnitOfMsr\" AS UND, NULL AS CANTIDAD, \"StdCost1\" AS COSTO, NULL AS COSTO_UNO, 0 AS STOCK, NULL AS WARECOD FROM SBO_TECNO_PRODUCCION.ORSC WHERE \"ResCode\" LIKE '%REPROCESO%';".format(od, od, od)
        self.openConnection()
        self.cursor.execute(sql)
        for row in self.cursor:
            self.selectArray.append(row)
        self.closeCursor()
        return self.selectArray

        # SELECT '0.0' AS NroOrden, 'ARTICULO' AS TIPO, OTM.\"ItemCode\" AS Codigo_Det, OTM.\"ItemName\" AS Nombre_Det, 'UND' AS Und, 1 AS Cantidad, 1 AS Costo, 
        #1 AS costo_Uno, 0 AS Stock, 'Undefined' AS WareCod FROM SBO_TECNO_PRODUCCION.OITM AS OTM WHERE \"ItemCode\" = '{}' 
        #UNION 
        # SELECT concat('1.' , TO_VARCHAR("ChildNum")) as NroOrden, IFNULL(CASE WHEN "Type" = '290' THEN 'ARTICULO' WHEN "Type" = '04' 
        # THEN 'RECURSO' ELSE 'OTROS' END ,'OTROS') AS TIPO, RD."Code" as Codigo_Det, RD."ItemName" as Nombre_Det, It."InvntryUom" AS Und, "Quantity" as 
        # Cantidad, CASE WHEN RD."Code" LIKE '%GGRC%' THEN It."AvgPrice" ELSE It."LastPurPrc" END as Costo, (SELECT "LastPurPrc" FROM SBO_TECNO_PRODUCCION.OITM OIT 
        # WHERE OIT."ItemCode" = RD."Father") AS costo_Uno, 0 AS Stock, 'Undefined' AS WareCod FROM SBO_TECNO_PRODUCCION.ITT1 RD LEFT JOIN SBO_TECNO_PRODUCCION.OITM It 
        # ON It."ItemCode" = RD."Code" WHERE RD."Father" = '{}' AND RD."Code" NOT LIKE '%MOH%' AND RD."Code" NOT LIKE '%MOM%';

    #obtiene lista de materiales por patron del almacen que tiene ultimo precio del maestro
    #por defecto esta consulta trae solo 10 filas
    def getMaterialfromWare(self, ware: str, searchPattern: str ='', offset: int = 0):
        self.selectArray.clear()
        sql = "SELECT '-' AS NroOrden, IFNULL(CASE WHEN itt1.\"Type\" = '290' THEN 'RECURSO' WHEN \"Type\" = '04' THEN 'ARTICULO' ELSE 'OTROS' END ,'OTROS') AS TIPO, oitw.\"ItemCode\" AS Codigo_Det, oitm.\"ItemName\" AS Nombre_Det, oitm.\"InvntryUom\" AS Und, NULL AS Cantidad, oitm.\"LastPurPrc\" AS Costo FROM SBO_TECNO_PRODUCCION.OITW oitw LEFT JOIN SBO_TECNO_PRODUCCION.OITM oitm ON oitw.\"ItemCode\" = oitm.\"ItemCode\" LEFT JOIN SBO_TECNO_PRODUCCION.ITT1 itt1 ON oitw.\"ItemCode\" = itt1.\"Code\" WHERE \"WhsCode\" like '%{}%' AND oitm.\"LastPurPrc\"  > 0  AND UPPER(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(CONCAT(oitw.\"ItemCode\", oitm.\"ItemName\"), 'Á', 'A'), 'É', 'E'), 'Í', 'I'), 'Ó', 'O'), 'Ú', 'U')) LIKE UPPER(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE('%{}%', 'Á', 'A'), 'É', 'E'), 'Í', 'I'), 'Ó', 'O'), 'Ú', 'U')) GROUP BY itt1.\"Type\", oitw.\"ItemCode\", oitm.\"ItemName\",oitm.\"LastPurPrc\",oitm.\"InvntryUom\" LIMIT 10 OFFSET {};".format(ware.upper(), searchPattern.upper(), str(offset))
        self.openConnection()
        self.cursor.execute(sql)
        for row in self.cursor:
            self.selectArray.append(row)
        self.closeCursor()
        return self.selectArray
        # CONCAT(oitw.\"ItemCode\", oitm.\"ItemName\") LIKE '%{}%' 

    #obtiene materiales por codigo o por nombre de material
    def getMaterialsByPattern(self, pattern: str = None):
        self.selectArray.clear()
        sql = "SELECT CONCAT(CONCAT(ITT.\"Code\", ' - '), ITT.\"ItemName\"), MAX(ITT.\"Quantity\") AS Quantity, MAX(ITT.\"Price\") AS Price, OTT.\"InvntryUom\" AS Und, IFNULL(CASE WHEN ITT.\"Type\" = '290' THEN 'RECURSO' WHEN \"Type\" = '04' THEN 'ARTICULO' ELSE 'OTROS' END ,'OTROS') AS TIPO FROM SBO_TECNO_PRODUCCION.ITT1 ITT LEFT JOIN SBO_TECNO_PRODUCCION.OITM OTT ON ITT.\"Code\" = OTT.\"ItemCode\" WHERE concat(ITT.\"Code\", ITT.\"ItemName\") like '%{}%' GROUP BY ITT.\"Code\", ITT.\"ItemName\", OTT.\"InvntryUom\", ITT.\"Type\";".format(pattern.upper())
        self.openConnection()
        self.cursor.execute(sql)
        for row in self.cursor:
            self.selectArray.append(row)
        self.closeCursor()
        return self.selectArray

    #obtiene todos los materiales en general
    def getMaterials(self):
        self.selectArray.clear()
        sql = "SELECT top 50 ITT.\"Code\" AS Code, ITT.\"ItemName\" AS ItemName, MAX(ITT.\"Quantity\") AS Quantity, MAX(ITT.\"Price\") AS Price, OTT.\"InvntryUom\" AS Und, IFNULL(CASE WHEN ITT.\"Type\" = '290' THEN 'RECURSO' WHEN \"Type\" = '04' THEN 'ARTICULO' ELSE 'OTROS' END ,'OTROS') AS TIPO FROM SBO_TECNO_PRODUCCION.ITT1 ITT LEFT JOIN SBO_TECNO_PRODUCCION.OITM OTT ON ITT.\"Code\" = OTT.\"ItemCode\" GROUP BY ITT.\"Code\", ITT.\"ItemName\", OTT.\"InvntryUom\", ITT.\"Type\";"
        self.openConnection()
        self.cursor.execute(sql)
        for row in self.cursor:
            self.selectArray.append(row)
        self.closeCursor()
        return self.selectArray


    #obtiene todos los articulos que pertenecen a una sub area de produccion
    def get_Articulo_Area(self, members: list = []):
      init_str = "OPRC.\"PrcCode\" = "
      members_str = " OR OPRC.\"PrcCode\" = ".join(f"'{x}'" for x in members)
      last_member_str = init_str + members_str
      self.selectArray.clear()
      sql = f"SELECT itt1.\"Father\", oit.\"ItemName\", oprc.\"PrcCode\" FROM SBO_TECNO_PRODUCCION.ITT1 itt1 INNER JOIN SBO_TECNO_PRODUCCION.OPRC oprc ON itt1.\"OcrCode2\" = oprc.\"PrcCode\" INNER JOIN SBO_TECNO_PRODUCCION.OITM oit ON itt1.\"Father\" = oit.\"ItemCode\" WHERE {last_member_str} GROUP BY itt1.\"Father\", oit.\"ItemName\", oprc.\"PrcCode\";"
      self.openConnection()
      self.cursor.execute(sql)
      for row in self.cursor:
          self.selectArray.append(row)
      self.closeCursor()
      return self.selectArray

    #obtiene todas las subareas del area de produccion
    def get_areas(self):
      self.selectArray.clear()
      sql = f"SELECT \"PrcCode\", \"PrcName\" FROM SBO_TECNO_PRODUCCION.OPRC WHERE \"PrcCode\" LIKE '%20001%' AND \"DimCode\" = 2 AND \"Active\" = 'Y' AND \"PrcCode\" != '2000103' AND \"PrcCode\" != '2000104' ORDER BY \"PrcCode\";"
      
      self.openConnection()
      self.cursor.execute(sql)
      for row in self.cursor:
          self.selectArray.append(row)
      self.closeCursor()
      return self.selectArray

    #obtiene los materiales por subarea de produccion
    def get_material_subarea(self, PrcCode):
      init_str = " OPRC.\"PrcCode\" = "
      members_str = " OR OPRC.\"PrcCode\" = ".join(f"'{x}'" for x in PrcCode)
      last_member_str = init_str + members_str
      self.selectArray.clear()
      sql = f"(SELECT itt1.\"Code\" AS Code, oitm.\"ItemName\" AS itemName, itt1.\"Quantity\" AS Quantity, oitm.\"LastPurPrc\" AS Price, oitm.\"InvntryUom\" AS Und, IFNULL(CASE WHEN itt1.\"Type\" = '290' THEN 'RECURSO' WHEN \"Type\" = '04' THEN 'ARTICULO' ELSE 'OTROS' END ,'OTROS') AS TIPO FROM SBO_TECNO_PRODUCCION.ITT1 itt1 INNER JOIN SBO_TECNO_PRODUCCION.OPRC oprc ON itt1.\"OcrCode2\" = oprc.\"PrcCode\" INNER JOIN SBO_TECNO_PRODUCCION.OITM oitm ON itt1.\"Code\" = oitm.\"ItemCode\" WHERE ({last_member_str}) AND itt1.\"Code\" NOT LIKE '%GGR%' GROUP BY itt1.\"Code\", oitm.\"ItemName\", itt1.\"Quantity\", oitm.\"LastPurPrc\", oitm.\"InvntryUom\", itt1.\"Type\") UNION (SELECT itt1.\"Code\" AS Code, oitm.\"ItemName\" AS itemName, NULL AS Quantity, oitm.\"AvgPrice\" AS Price, oitm.\"InvntryUom\" AS Und, IFNULL(CASE WHEN itt1.\"Type\" = '290' THEN 'RECURSO' WHEN \"Type\" = '04' THEN 'ARTICULO' ELSE 'OTROS' END ,'OTROS') AS TIPO FROM SBO_TECNO_PRODUCCION.ITT1 itt1 INNER JOIN SBO_TECNO_PRODUCCION.OPRC oprc ON itt1.\"OcrCode2\" = oprc.\"PrcCode\" INNER JOIN SBO_TECNO_PRODUCCION.OITM oitm ON itt1.\"Code\" = oitm.\"ItemCode\" WHERE ({last_member_str}) AND itt1.\"Code\" LIKE '%GGR%' GROUP BY itt1.\"Code\", oitm.\"ItemName\", oitm.\"AvgPrice\", oitm.\"InvntryUom\", itt1.\"Type\");"
      self.openConnection()
      self.cursor.execute(sql)
      for row in self.cursor:
          self.selectArray.append(row)
      self.closeCursor()
      return self.selectArray
    
    #obtiene el almacen que no son observados ni reproceso por subarea
    def get_warehouse_by_subarea(self, subarea):
      self.selectArray.clear()
      sql = f"SELECT * FROM (SELECT \"WhsCode\" AS WareCode, \"WhsName\" AS WareName FROM SBO_TECNO_PRODUCCION.OWHS owhs WHERE UPPER(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(owhs.\"WhsName\", 'Á', 'A'), 'É', 'E'), 'Í', 'I'), 'Ó', 'O'), 'Ú', 'U')) LIKE UPPER(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE('%{subarea}%', 'Á', 'A'), 'É', 'E'), 'Í', 'I'), 'Ó', 'O'),'Ú', 'U')) AND owhs.\"Inactive\" = 'N') AS F WHERE F.WareName NOT LIKE '%OBSERVADO%' AND F.WareName NOT LIKE '%PROCESO%'"
      self.openConnection()
      self.cursor.execute(sql)
      for row in self.cursor:
          self.selectArray.append(row)
      self.closeCursor()
      return self.selectArray
 
    #obtiene canttidad de rechados de producto en subarea
    def get_fail_product_by_subarea(self, odArticle: str = "", subareaPattern: str = ""):
      self.selectArray.clear()
      sql = f"SELECT F.Stock AS Stock, F.WareCod AS WareCod FROM (SELECT table1.\"WhsCode\" AS WareCod, \"OnHand\" AS Stock FROM SBO_TECNO_PRODUCCION.OITW table1 RIGHT JOIN SBO_TECNO_PRODUCCION.OWHS table2 ON table1.\"WhsCode\" = table2.\"WhsCode\" WHERE \"ItemCode\" = '{odArticle}' AND table2.\"WhsName\" LIKE '%OBSERVADOS%' AND UPPER(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(table2.\"WhsName\", 'Á', 'A'), 'É', 'E'), 'Í', 'I'), 'Ó', 'O'), 'Ú', 'U')) LIKE UPPER(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE('%{subareaPattern}%', 'Á', 'A'), 'É', 'E'), 'Í', 'I'), 'Ó', 'O'), 'Ú', 'U')) GROUP BY table1.\"WhsCode\", \"OnHand\") AS F"
      self.openConnection()
      self.cursor.execute(sql)
      for row in self.cursor:
          self.selectArray.append(row)
      self.closeCursor()
      return self.selectArray
