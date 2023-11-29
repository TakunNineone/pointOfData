import itertools,heapq,math,openpyxl

import pandas as pd,numpy as np,pickle,re,datetime,os,psycopg2,warnings
warnings.filterwarnings("ignore")

class pointOfData():
    def __init__(self):
        None

    def print_d(self,text):
        print(f'{datetime.datetime.now()} - {text}')

    def connect_to_bd(self,version="final_6",user="postgres",password="124kosm21",host="127.0.0.1",port="5432"):
        self.connect = psycopg2.connect(user=user,
                                        password=password,
                                        host=host,
                                        port=port,
                                        database=version)

    def find_common_eps(self,group):
        eps = group['eps'].str.split(';')
        common = set(eps.iloc[0])
        for ep in eps:
            common = common.intersection(set(ep))
        return ';'.join(common) if common else 'нет совпадений ep'

    def nearest_first_product(self,*sequences):
        start = (0,) * len(sequences)
        queue = [(0, start)]
        seen = set([start])
        while queue:
            priority, indexes = heapq.heappop(queue)
            yield list(seq[index] for seq, index in zip(sequences, indexes))
            for i in range(len(sequences)):
                if indexes[i] < len(sequences[i]) - 1:
                    lst = list(indexes)
                    lst[i] += 1
                    new_indexes = tuple(lst)
                    if new_indexes not in seen:
                        new_priority = sum(index * index for index in new_indexes)
                        heapq.heappush(queue, (new_priority, new_indexes))
                        seen.add(new_indexes)

    def save_large_dataframe_to_excel(self, df, excel_file_path, max_rows_per_sheet=1048575,ep=None):
        if ep!=None:
            df.insert(0, "entrypoint",ep)
        num_chunks = len(df) // max_rows_per_sheet + 1
        for i in range(num_chunks):
            path = excel_file_path.split('.xslx')[0] + f'_{i}' + '.xlsx'
            with pd.ExcelWriter(path) as writer:
                print('сохраняю книгу', i)
                start_row = i * max_rows_per_sheet
                end_row = (i + 1) * max_rows_per_sheet
                chunk_df = df[start_row:end_row]
                chunk_df.to_excel(writer, sheet_name=f'Sheet', index=False)
                print(f'Сохранено в {path}')


    def get_points(self,sql):
        data_df = pd.read_sql_query(sql, self.connect)
        data_df['entity']=[xx.replace('-definition.xml','') for xx in data_df['entity']]
        data_df_wo_concept=data_df[['rinok','entity','parentrole','dims']]
        data_df_wo_concept['dims']=[';'.join(xx) for xx in data_df_wo_concept['dims']]
        data_df_wo_concept=data_df_wo_concept.drop_duplicates()
        data_df_wo_concept=data_df_wo_concept.reset_index()
        data_df_wo_concept['dims']=[xx.split(';') for xx in data_df_wo_concept['dims']]
        dims=data_df_wo_concept['dims']
        data=[]


        for idx,xx in enumerate(dims):
            dims[idx]=[yy.split('|') for yy in xx]
            lens=math.prod([len(yy) for yy in dims[idx]])
            if lens<10000000:

                i = 0
                for yy in self.nearest_first_product(*dims[idx]):
                    start = datetime.datetime.now()
                    yy.sort()
                    arr_temp=';'.join(yy)
                    data.append([data_df_wo_concept['rinok'][idx],data_df_wo_concept['entity'][idx],data_df_wo_concept['parentrole'][idx],arr_temp])
                    i+=1
                    finish = datetime.datetime.now()
                    print("", end=f"\rPercentComplete:{i+1} {round((i + 1) / lens * 100, 2)}%, time: {finish - start}")
            else:
                with open('skip_roles.txt','a') as f:
                    f.write(data_df_wo_concept['parentrole'][idx]+'\n')

        columns = ['rinok','entity', 'parentrole', 'dims_n']
        df_res=pd.DataFrame(data=data,columns=columns)

        df_res = pd.merge(df_res, data_df, on=['parentrole','rinok','entity'])
        print(df_res.keys())

        df_res = pd.DataFrame({'parentrole_agg': df_res.groupby(['rinok','dims_n','concept'])['parentrole'].aggregate(lambda x: list(x)),
                               'entity_agg': df_res.groupby(['rinok', 'dims_n', 'concept'])['entity'].aggregate(lambda x: ';'.join(list(x))),
                               #'ep_agg': df_res.groupby(['rinok', 'dims_n', 'concept'])['eps'].aggregate(lambda x: ';'.join(list(x)))
                              #'ep_agg': df_res.groupby(['rinok', 'dims_n', 'concept'])['eps'].agg(pd.Series.mode)
                               # 'ep_agg': df_res.groupby(['rinok', 'dims_n', 'concept'])['eps'].aggregate(lambda x:pd.Series.mode(x)[0])
                               'ep_agg': df_res.groupby(['rinok', 'dims_n', 'concept']).apply(self.find_common_eps)
                               }).reset_index()

        df_res['duble'] = [len(xx) for xx in df_res['parentrole_agg']]
        df_res['parentrole_agg']=[';'.join(xx) for xx in df_res['parentrole_agg']]
        # df_res['ep_agg'] = [ ';'.join(list(set(xx.split(';')))) for xx in df_res['ep_agg']]
        df_res['entity_agg'] = [';'.join(list(set(xx.split(';')))) for xx in df_res['entity_agg']]
        df_res['entity_cnt'] = [len(xx.split(';')) for xx in df_res['entity_agg']]
        df_res=df_res[df_res['duble']>1]


        self.save_large_dataframe_to_excel(df_res, '41_allNSO')


if __name__ == "__main__":
    ss=pointOfData()
    ss.connect_to_bd()
    with open('sql_data.sql','r') as f:
        sql=f.read()
    ss.get_points(sql)