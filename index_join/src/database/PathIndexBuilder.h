//
// Created by Qingzhi Ma.
// Department of Computer Science
// University of Warwick
// Email: Q.Ma.2@warwick.ac.uk
// Copyright (c) 2019 University of Warwick. All rights reserved.
//

#ifndef MAIN_MEM_JOIN_PATHINDEXBUILDER_H
#define MAIN_MEM_JOIN_PATHINDEXBUILDER_H


#include <cstdint>
#include <map>
#include <vector>
#include "JoinPath.h"
#include "Table.h"


struct PathIndexContainer{
    int64_t count;
    int dimension;
    std::shared_ptr<std::map<int, std::vector<int64_t>>> indexes;
};

class PathIndex{
private:
    std::shared_ptr<int64_t> m_count;
    int m_dimension;
    /*JoinPath m_joinPath;*/
    std::string m_joinPath;
public:
    std::shared_ptr<std::map<int, std::vector<int64_t>>> m_indexes;
public:
    /*PathIndex(JoinPath joinPath, int dimension) :m_joinPath(joinPath), m_dimension(dimension){};*/
    PathIndex(std::string joinPath, int dimension) :m_joinPath(joinPath), m_dimension(dimension){
        m_indexes = std::make_shared<std::map<int, std::vector<int64_t>>>();
    };



    /*void initializeLeftTable(std::shared_ptr < key_index > keyIndex){
        *//*std::pair <std::multimap<int64_t,int64_t>::iterator, std::multimap<int64_t,int64_t>::iterator> ret;
        *//**//*key_index::iterator ret;*//**//*
        switch (m_joinPath.getJoinPath()->at(0).second){
            case DATABASE_DATA_TYPES::FLOAT :
                ret = keyIndex->equal_range(std::stof(m_joinPath.getJoinPath()->at(0).first));
                break;
            case DATABASE_DATA_TYPES::STRING:
                ret = keyIndex->equal_range(m_joinPath.getJoinPath()->at(0).first);
                break;
            default:
                throw std::runtime_error("fail to initialize the left-most table");
        }*//*



        for (key_index::iterator it = keyIndex->begin(); it!= keyIndex->end(); it++){
            (*m_indexes)[0].emplace_back(it->second);
        }
    };

    void addIntermediateTable(std::shared_ptr< composite_key_index > keyIndex, int tableNum){

    };*/
};
#endif //MAIN_MEM_JOIN_PATHINDEXBUILDER_H
