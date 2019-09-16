//
// Created by Qingzhi Ma.
// Department of Computer Science
// University of Warwick
// Email: Q.Ma.2@warwick.ac.uk
// Copyright (c) 2019 University of Warwick. All rights reserved.
//

#ifndef MAIN_MEM_JOIN_JOINPATH_H
#define MAIN_MEM_JOIN_JOINPATH_H

#include <vector>
#include <string>
#include <exception>
#include <sstream>
#include <iostream>
#include "DatabaseSharedTypes.h"

typedef std::pair<std::string, DATABASE_DATA_TYPES> PathNode;
//typedef  int64_t PathNode;

class JoinPath{
public:
    std::shared_ptr<std::vector<PathNode>> m_joinPath;
public:
    JoinPath(){m_joinPath=std::make_shared<std::vector<PathNode>>();};
    JoinPath(std::string _value, DATABASE_DATA_TYPES _type){
        PathNode pathNode(_value, _type);
        JoinPath();
        addNode((pathNode));
    };
    ~JoinPath(){};
    void addNode(PathNode  pathNode) const{
        m_joinPath->push_back(pathNode);
    };

    /*PathNode getLastNode(){
        return m_joinPath->back();
    };

    std::string toString(){
        std::string outputStr ="";
        for (PathNode pathNode:*m_joinPath){
            outputStr+=pathNode + "->";
        }
        outputStr.pop_back();
        outputStr.pop_back();
        return outputStr;
    };*/




    void addNode(std::string value, DATABASE_DATA_TYPES databaseDataTypes){
        PathNode pathNode(value,databaseDataTypes);
        addNode(pathNode);
    }

    float getLastNodeFloat(){
        if (m_joinPath->back().second ==DATABASE_DATA_TYPES::FLOAT)
            return std::stof(m_joinPath->back().first);
        else
            throw std::runtime_error("the node has unexpected data type!");
    }

    int64_t getLastNodeInt64() const {
        /*if(m_joinPath->back().second == DATABASE_DATA_TYPES::INT64)*/
        return std::stoll(m_joinPath->back().first);
        /*else
            throw std::runtime_error("the node has unexpected data type!");*/
    }

    std::string getLastNodeString(){
        if (m_joinPath->back().second ==DATABASE_DATA_TYPES::STRING)
            return m_joinPath->back().first;
        else
            throw std::runtime_error("the node has unexpected data type!");
    }

    std::string toString(){
        std::string outputStr ="";
        for (PathNode pathNode:*m_joinPath){
            outputStr+=pathNode.first+"->";
        }
        outputStr.pop_back();
        outputStr.pop_back();
        return outputStr;
    }

    std::shared_ptr<std::vector<PathNode>> getJoinPath(){
        return m_joinPath;
    }

};
#endif //MAIN_MEM_JOIN_JOINPATH_H
