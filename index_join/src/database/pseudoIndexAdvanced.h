//
// Created by Qingzhi Ma.
// Department of Computer Science
// University of Warwick
// Email: Q.Ma.2@warwick.ac.uk
// Copyright (c) 2019 University of Warwick. All rights reserved.
//

#ifndef MAIN_MEM_JOIN_PSEUDOINDEXADVANCED_H
#define MAIN_MEM_JOIN_PSEUDOINDEXADVANCED_H

#include <vector>
#include <list>
#include <map>
#include <string>
#include <random>

#include "Table.h"
#include "JoinOutputColumnContainer.h"
#include "JoinPath.h"
#include "PathIndexBuilder.h"


//typedef std::shared_ptr<std::map<int, int64_t>> JoinIndexItemIndexes;
typedef std::map<int, int64_t> JoinIndexItemIndexes;

struct multimapComp{
    bool operator()(JoinPath &key_lhs, JoinPath &key_rhs) const {
        if (key_lhs.m_joinPath->size() <  key_rhs.m_joinPath->size()) {
            return true;
        } else //(key_lhs.m_joinPath->size() <  key_rhs.m_joinPath->size())
        {
            return false;
        }
        /*} else{
            bool if_less = true;
            for(int i=0;i<key_lhs.m_joinPath->size(); i++){
                if_less = if_less && (key_lhs.m_joinPath->at(i)< key_rhs.m_joinPath->at(i));
            }
            return if_less;
        }*/
    }
};

class PseudoIndexAdvancedBuilder {
public:
    PseudoIndexAdvancedBuilder();
    ~PseudoIndexAdvancedBuilder(){};
    int AppendTable(std::shared_ptr<Table> table, int RHSIndex, int LHSIndex, int tableNumber);
    void Build();
    void Sample(int sampleSize);
    void Sample(int sampleSize, JoinOutputColumnContainer joinOutputColumnContainer);
    int64_t getCardinality();

private:
    int n_tables;
    int sampleSize;
    std::vector<std::shared_ptr<Table> > m_joinedTables;
    std::vector<int> m_LHSJoinIndex;
    std::vector<int> m_RHSJoinIndex;
    /*std::map<std::string, int64_t> m_join_counts;*/
    /*std::shared_ptr<std::map<JoinPath, PathIndex, multimapComp>> m_pathIndexes;*/
    std::shared_ptr<std::map<std::string, PathIndex>> m_pathIndexes;

    std::map<int64_t , std::pair<std::string, int64_t >> m_join_counts_acc;

    int64_t m_cadinality;

    struct JoinIndexItem{
        int64_t tag;                /* the composite index key*/
        std::int64_t offsetNum;     /* record the offset from the right most record.*/
        int64_t leftKey;
        std::vector<std::tuple<int64_t , int64_t >> midKeys;
        int64_t  rightKey;
        int64_t pseudoIndex;
    };

    std::map<int64_t, std::vector<JoinIndexItem>> m_sampleIndexContainer;

private:
    JoinIndexItem getJoinIndexItem(int64_t num);
    JoinIndexItemIndexes getJoinIndexItemIndexes(JoinIndexItem joinIndexItem);
    void fetchJoinTuples(std::string outfile);
    void fetchJoinTuples(std::string outfile, JoinOutputColumnContainer joinOutputColumnContainer);
    std::string fetchJoinTupleRandom(JoinIndexItem joinIndexItem);

    std::string fetchJoinTupleUsingIndex(JoinIndexItemIndexes joinIndexItemIndexes, JoinOutputColumnContainer joinOutputColumnContainer);
    std::string fetchJoinTupleUsingIndex(JoinIndexItemIndexes joinIndexItemIndexes, JoinOutputColumnContainer joinOutputColumnContainer, int64_t pseudoIndex);

//    std::bidirectional_iterator_tag getIterator(std::shared_ptr < key_index > maps,int64_t loc);


};
#endif //MAIN_MEM_JOIN_PSEUDOINDEXADVANCED_H
