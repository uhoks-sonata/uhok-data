import os
import pandas as pd
import mariadb
import json

# list = [1,2,3,4,5]
# dict_d = {"star":["☆","★"]}
data = {"info":[{"document_id":"60606"}],"annotation":[{"contents_title":"제 1 장","QnA":[{"question_id":1,"question_format":"주관식","role":"고용 및 노동 환경","question_type":"정의형","instruction":"직제규정 제14조에 따르면 유통관리부의 업무는 무엇입니까?","input":[{"op1":"", "op2":"","op3":[{"po1":"","po2":[{"bo1":""}],"po3":""}],"op4":""}],"output":"가. 유통이력사업 사업계획 수립 및 사업평가에 관한 사항 나. 유통이력사업 대내외 회의 및 행사에 관한 사항 다. 유통이력사업 제도개선에 관한 사항 라. 소관업무 회의, 행사에 관한 사항 마. 소관업무 법령 및 제도개선에 관한 사항 바. 소관업무 광고 및 홍보물제작 배부에 관한사항 사. 소관업무 언론보도 대응 및 보도자료 배부에 관한사항 아. 소관부서 예산편성에 관한사항"}]}]}

def print_json_structure(data, indent=0):
    if isinstance(data, dict):
        for key, value in data.items():
            print('    ' * indent + str(key))
            print_json_structure(value, indent + 1)
    elif isinstance(data, list):
        if data: 
            print_json_structure(data[0], indent)

if __name__ == "__main__":
    print_json_structure(data)