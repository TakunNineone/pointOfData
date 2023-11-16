import itertools,heapq,math,openpyxl

import pandas as pd,numpy as np,pickle,re,datetime,os,psycopg2,warnings
warnings.filterwarnings("ignore")

class pointOfData():
    def __init__(self):
        None

    def print_d(self,text):
        print(f'{datetime.datetime.now()} - {text}')

    def connect_to_bd(self,version="final_6_5_idx",user="postgres",password="124kosm21",host="127.0.0.1",port="5432"):
        self.connect = psycopg2.connect(user=user,
                                        password=password,
                                        host=host,
                                        port=port,
                                        database=version)

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
        dims=data_df['dims']
        data=[]
        i=0
        columns=['parentrole','concept','dims_n']

        for idx,xx in enumerate(dims):
            dims[idx]=[yy.split('|') for yy in xx]
            lens=math.prod([len(yy) for yy in dims[idx]])
            # print(lens)
            # print(idx)
            if lens<10000000:
                i=0
                start = datetime.datetime.now()
                for yy in self.nearest_first_product(*dims[idx]):
                    yy.sort()
                    arr_temp=';'.join(yy)
                    data.append([data_df['parentrole'][idx],data_df['concept'][idx],arr_temp])
                    i+=1
                    finish = datetime.datetime.now()
                    print("", end=f"\rPercentComplete:{i+1} {round((i + 1) / lens * 100, 2)}%, time: {finish - start}")
                print('\n')
            else:
                print(data_df['parentrole'][idx])
            df_res=pd.DataFrame(data=data,columns=columns)
        # self.save_large_dataframe_to_excel(df_res, 'df_res')

        df_res = pd.merge(df_res, data_df, on=['parentrole','concept'])

        df_res = pd.DataFrame({'parentrole_agg': df_res.groupby(['dims_n', 'concept'])['parentrole'].aggregate(lambda x: list(x))}).reset_index()

        df_res['duble'] = [len(xx) for xx in df_res['parentrole_agg']]
        df_res=df_res[df_res['duble']>1]

        self.save_large_dataframe_to_excel(df_res, 'temp')


if __name__ == "__main__":
    ss=pointOfData()
    ss.connect_to_bd()
    with open('sql_data.sql','r') as f:
        sql=f.read()
    ss.get_points(sql)