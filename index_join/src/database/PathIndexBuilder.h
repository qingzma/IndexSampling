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

class PathIndexBuilder{
private:
    int64_t m_count;
    int m_dimension;
    std::shared_ptr<std::map<int, std::vector<int64_t>>> m_indexes;
    JoinPath m_joinPath;
public:
    PathIndexBuilder(JoinPath joinPath, int dimension=0) :m_joinPath(joinPath), m_dimension(dimension){};

    void initializeLeft(std::shared_ptr < key_index > keyIndex){
        /*std::pair <std::multimap<int64_t,int64_t>::iterator, std::multimap<int64_t,int64_t>::iterator> ret;
        *//*key_index::iterator ret;*//*
        switch (m_joinPath.getJoinPath()->at(0).second){
            case DATABASE_DATA_TYPES::FLOAT :
                ret = keyIndex->equal_range(std::stof(m_joinPath.getJoinPath()->at(0).first));
                break;
            case DATABASE_DATA_TYPES::STRING:
                ret = keyIndex->equal_range(m_joinPath.getJoinPath()->at(0).first);
                break;
            default:
                throw std::runtime_error("fail to initialize the left-most table");
        }*/

        for (key_index::iterator it = keyIndex->begin(); it!= keyIndex->end(); it++){
            (*m_indexes)[0].emplace_back(it->second);
        }
    };
};
#endif //MAIN_MEM_JOIN_PATHINDEXBUILDER_H
