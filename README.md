## MySQL to Neo4j

* * *

### 設定檔參數

* description: <Graph說明>
* serverVersion: <版本>
* offset: <MySQL分頁查詢用>
* limit: <MySQL分頁查詢用>
* nodes: <新增節點設定>
    
    * env: <來源資料庫環境設定>
    * model_name: <ORM定義名稱>
    * formatter_conf: <定義輸出資料欄位>
        * model_name: <Formatter名稱>
        * attrs: <formatter 屬性名稱>
    * df_action_map: <Dataframe操作>
    * mode: <Query 條件 and, or ...>
    * merge_keys: <Node Releation對應key>
    * node_label: <節點Label>

* relations: <新增節點間關聯設計>
    * env: <來源資料庫環境設定> 
    * model_name: <ORM定義名稱>
    * relation_name: <關聯名稱> 
    * start_key: <Node關聯對應key>  
    * end_key: <Node關聯對應key>
    * formatter_conf: <定義輸出資料欄位>
        * model_name: <Formatter名稱>
        * attrs: <formatter 屬性名稱>
    * start_node_key: <起始節點, 對應Node屬性>
      - <屬性1> 
      - <屬性2> 
      - <屬性3> 
    * end_node_key: <結束節點, 對應Node屬性>
      - <屬性1> 
      - <屬性2> 
      - <屬性3> 

* * *
