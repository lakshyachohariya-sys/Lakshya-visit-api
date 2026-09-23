from flask import Flask,jsonify
import aiohttp,asyncio,json
from byte import encrypt_api,Encrypt_ID
from visit_count_pb2 import Info
app=Flask(__name__)
def load_tokens(server_name):
    try:
        if server_name=="IND":
            path="token_ind.json"
        elif server_name in {"BR","US","SAC","NA"}:
            path="token_br.json"
        else:
            path="token_bd.json"
        with open(path,"r") as f:
            data=json.load(f)
        return [item["token"] for item in data if "token" in item and item["token"] not in ["","N/A"]]
    except Exception:
        return []
def get_url(server_name):
    if server_name=="IND":
        return "https://client.ind.freefiremobile.com/GetPlayerPersonalShow"
    if server_name in {"BR","US","SAC","NA"}:
        return "https://client.us.freefiremobile.com/GetPlayerPersonalShow"
    return "https://clientbp.ggpolarbear.com/GetPlayerPersonalShow"
def parse_protobuf_response(response_data):
    try:
        info=Info()
        info.ParseFromString(response_data)
        return {"uid":info.AccountInfo.UID if info.AccountInfo.UID else 0,"nickname":info.AccountInfo.PlayerNickname if info.AccountInfo.PlayerNickname else "","likes":info.AccountInfo.Likes if info.AccountInfo.Likes else 0,"region":info.AccountInfo.PlayerRegion if info.AccountInfo.PlayerRegion else "","level":info.AccountInfo.Levels if info.AccountInfo.Levels else 0}
    except Exception:
        return None
async def visit(session,url,token,uid,data):
    headers={"ReleaseVersion":"OB55","X-GA":"v1 1","Authorization":f"Bearer {token}","Host":url.replace("https://","").split("/")[0]}
    try:
        async with session.post(url,headers=headers,data=data,ssl=False) as resp:
            if resp.status==200:
                return True,await resp.read()
            return False,None
    except Exception:
        return False,None
async def send_until_target(tokens,uid,server_name,target_success=10000):
    url=get_url(server_name)
    connector=aiohttp.TCPConnector(limit=0)
    total_success=0
    total_sent=0
    player_info=None
    async with aiohttp.ClientSession(connector=connector) as session:
        data=bytes.fromhex(encrypt_api("08"+Encrypt_ID(str(uid))+"1801"))
        while total_success<target_success:
            batch_size=min(target_success-total_success,300)
            tasks=[asyncio.create_task(visit(session,url,tokens[(total_sent+i)%len(tokens)],uid,data)) for i in range(batch_size)]
            results=await asyncio.gather(*tasks)
            if player_info is None:
                for success,response in results:
                    if success and response is not None:
                        player_info=parse_protobuf_response(response)
                        break
            batch_success=sum(1 for r,_ in results if r)
            total_success+=batch_success
            total_sent+=batch_size
    return total_success,total_sent,player_info
@app.route('/<string:server>/<int:uid>',methods=['GET'])
def send_visits(server,uid):
    server=server.upper()
    tokens=load_tokens(server)
    target_success=10000
    if not tokens:
        return jsonify({"error":"No valid tokens found"}),500
    total_success,total_sent,player_info=asyncio.run(send_until_target(tokens,uid,server,target_success=target_success))
    if player_info:
        return jsonify({"fail":target_success-total_success,"level":player_info.get("level",0),"likes":player_info.get("likes",0),"nickname":player_info.get("nickname",""),"region":player_info.get("region",""),"success":total_success,"uid":player_info.get("uid",0)}),200
    return jsonify({"error":"Could not decode player information"}),500
if __name__=="__main__":
    app.run(host="0.0.0.0",port=5000)