#!/usr/bin/env python
# coding: utf-8

# In[ ]:


import pandas as pd



# This function takes 2 dataframes which consists the data about the edge connections. It returns the edge
#     connections which have common predecessors 


def common_predecessors(df1,df2):

    df_common_references = pd.merge(df1,df2,on='cited_paper_id',how='inner') 
    column={
    'paper_id_x':'paper_id',
    'paper_id_y':'cited_paper_id'
    }
    df_common_references.drop(columns='cited_paper_id',inplace=True)
    df_common_references.rename(columns=column,inplace=True)
#     print(df_chain.head())
#     print(df_chain.shape)
    df_common_predecessor_1=pd.merge(df1,df_common_references,on=['paper_id','cited_paper_id'],how='inner')
    df_common_predecessor_2=pd.merge(df2,df_common_references,on=['paper_id','cited_paper_id'],how='inner')
    df_common_predecessor=pd.concat([df_common_predecessor_1,df_common_predecessor_2],ignore_index=True)
    df_common_predecessor.drop_duplicates(subset=['paper_id','cited_paper_id'],inplace=True,ignore_index=True)
    return df_common_predecessor






# As the data can be huge, this function is to obtain the edges with common predecessors by splitting the data into
# smaller subsets so that the system wouldn't crash 

def all_common_predecessors(df_edge,subset_length):

    df_common_predecessors=pd.DataFrame(columns=['paper_id','cited_paper_id']) # dataframe consisting the whole 
    num_edges = df_edge.shape[0]
#     print(num_edges)
    for i in range((num_edges//subset_length)+1):
        for j in range((num_edges//subset_length)+1):
            subset1=df_edge[(i)*subset_length:min(num_edges,(i+1)*subset_length)]
#             print(subset1)
            subset2=df_edge[(j)*subset_length:min(num_edges,(j+1)*subset_length)]
            tmp = common_predecessors(subset1,subset2)
            df_common_predecessors=pd.concat([df_common_predecessors,tmp],ignore_index=True)
    #         print(df_common_predecessor.shape)
    return df_common_predecessors








# Adding the published year of the paper and the reference paper in an edge connection
def add_paper_year(df_edge,df_year):

    df_edge=pd.merge(df_edge,df_year,on='paper_id',how='inner', sort=False)
    df_edge=pd.merge(df_edge,df_year,left_on='cited_paper_id',right_on='paper_id',how='inner', sort=False)
    df_edge.drop(columns='paper_id_y',inplace=True)
    column={'paper_id_x': 'paper_id'}
    df_edge.rename(columns=column, inplace=True)
    return df_edge






# Removing the citation edges which cited after t years of publication 

def year_threshold(df,t):
    df=df[df['year_x']-df['year_y']<=t]
    return df









def CD_t(df_edge,df_year,subset_length,t):
    # obtained the edges with common predecessors 
    df_common_predecessors = all_common_predecessors(df_edge,subset_length)
    #adding the published years of citing and reference papers to be able to set thresholds
    df_edge = add_paper_year(df_edge,df_year)
    df_common_predecessors = add_paper_year(df_common_predecessors,df_year)
    # removing the citations which have more than t years gap
    df_edge = year_threshold(df_edge,t)
    df_common_predecessors = year_threshold(df_common_predecessors,t)
    # calculating the number of citaions and citations with common predecessors of each paper
    df_total_cit_count = df_edge.value_counts(subset='cited_paper_id').reset_index()
    df_common_cit_count = df_common_predecessors.value_counts(subset='cited_paper_id').reset_index()
    df_cit_count = pd.merge(df_total_cit_count, df_common_cit_count, on = 'cited_paper_id', how='outer') 
    df_cit_count.fillna(0, inplace = True)
    column_renames = {
        'count_x' : 'total_cit_count',
        'count_y' : 'common_cit_count'
    }
    df_cit_count.rename(columns=column_renames, inplace=True)
    
    # CD calculation 
    df_cd = df_cit_count[df_cit_count['total_cit_count'] > 0]
    cd_t = 'cd'+str(t)

    df_cd[cd_t]= 1 - 2 * (df_cd['common_cit_count']/df_cd['total_cit_count'])
    
    return df_cd 



    

    


df_year = pd.read_csv('node_year.csv')
df_edge = pd.read_csv('edge_index.csv')
i = 5 # i in CD_i index
subset_length = 25000000 # choose this value based on your CPU memory. If the computer crashes then reduce the value
df_cdi = CD_t(df_edge,df_year,subset_length, i)
cd_i = 'cd'+str(i)+'.csv'
df_cdi.to_csv(cd_i, index=False)

    
    








