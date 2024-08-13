import requests
from decouple import config

COMPANYDB_SAP=config("COMPANYDB_SAP")
PASSWORD_SAP=config("PASSWORD_SAP")
USERNAME_SAP=config("USERNAME_SAP")
URL_SAP=config("URL_SAP")

class SLSConnection:
    def __init__(self):
        self.headers = {}
        self.session = requests.Session()
    
    def Login(self):
        self.headers.clear()
        login_url = f"{URL_SAP}/Login"
        payload = {
            "CompanyDB": COMPANYDB_SAP,
            "UserName": USERNAME_SAP,
            "Password": PASSWORD_SAP
        }
        #AQui tiene que ir un try catch
        response = self.session.post(login_url, json=payload, verify=False)
        if (response.status_code == 200):
            self.headers = {
                'Authorization': "Bearer " + response.json()["SessionId"]
                }
            return True
        else:
            return False

    def Logout(self):
        self.headers.clear()
        logout_url = f"{URL_SAP}/Logout"
        #AQui tiene que ir un try catch
        response = self.session.post(logout_url, headers=self.headers, verify=False)
        if (response.status_code == 200 or response.status_code == 204):
            return True
        else:
            return False

    def Get_ProductionOrderById(self, poId: str = ""):
        isLogin = self.Login()
        if isLogin:
            PO_B_Id_url = f"{URL_SAP}/ProductionOrders({poId})"
            response = self.session.get(PO_B_Id_url, headers=self.headers, verify=False)
            status_code = response.status_code
            response_json = response.json()
            self.Logout()
            if (status_code == 200):
                results = response_json
                return results
            else:
                return {"AbsoluteEntry": "Nothing to show you"}
        else:
            return {"Detail": "Nothing to show you"}

    def Post_ProductionOrder(self, model):
        isLogin = self.Login()
        if isLogin:
            try:
                payload = self.make_OF_Payload(model)
                PO_url = f"{URL_SAP}/ProductionOrders"
                response = self.session.post(PO_url, json=payload, headers=self.headers, verify=False)
                status_code = response.status_code
                response_json = response.json()
                self.Logout()
                if (status_code == 200 or status_code == 201):
                    return status_code, {"DocumentNumber": response_json["DocumentNumber"]}
                else:
                    return 500, {"DocumentNumber": "XXXXXX"}
            except Exception as error:
                self.Logout()
                print(f"An error ocurred:{error}")
                return 500, {"DocumentNumber": "XXXXXX"}
        else:
            return 500, {"DocumentNumber": "XXXXXX"}

    def make_OF_Payload(self, model):
        ProductionOrderLines = [{"ItemNo": x.ItemNo, "PlannedQuantity": x.PlannedQuantity, "ProductionOrderIssueType": x.ProductionOrderIssueType, "Warehouse": x.Warehouse, "DistributionRule": x.DistributionRule, "DistributionRule2": x.DistributionRule2, "ItemType": x.ItemType, "StartDate": x.StartDate, "EndDate": x.EndDate, "ItemName": x.ItemName} for x in model.ProductionOrderLines if bool(x)]
        payload = {
        "ItemNo": model.ItemCode,
        "ProductionOrderStatus": "boposPlanned",
        "ProductionOrderType": "bopotSpecial",
        "PlannedQuantity": model.PlannedQty,
        "PostingDate": model.PostDate,
        "DueDate": model.DueDate,
        "ProductionOrderOrigin": "bopooManual",
        "Warehouse": model.Warehouse,
        "InventoryUOM": "UND",
        "StartDate": model.StartDate,
        "ProductDescription": model.ProdName,
        "U_MSS_USESWC": model.Usuario_Codigo_SIM,
        "U_MSS_REDSWC": "Y",
        "ProductionOrderLines": ProductionOrderLines
        }
        return payload