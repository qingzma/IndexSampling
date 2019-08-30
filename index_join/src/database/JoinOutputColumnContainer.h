//
// Created by Qingzhi Ma.
// Department of Computer Science
// University of Warwick
// Email: Q.Ma.2@warwick.ac.uk
// Copyright (c) 2019 University of Warwick. All rights reserved.
//

#ifndef MAIN_MEM_JOIN_JOINOUTPUTCOLUMNCONTAINER_H
#define MAIN_MEM_JOIN_JOINOUTPUTCOLUMNCONTAINER_H

#include <memory>
#include <vector>
#include <set>
#include <map>

class JoinOutputColumnContainer{
private:
    /*std::shared_ptr<std::vector<std::pair<int, std::set<int>>>> m_container;*/
    std::shared_ptr<std::map<int, std::set<int>>> m_container;
public:
    JoinOutputColumnContainer(){
        m_container = std::make_shared<std::map<int, std::set<int>>>();
    }

    void addColumn(int table, int column){
        if (m_container->find(table) == m_container->end()){/* the table does not exist in the container, so create one*/
            m_container->insert(std::pair<int, std::set<int>>(table, std::set<int>{column}));
        }else{
            m_container->at(table).insert(column);
        }
    }

    std::shared_ptr<std::map<int, std::set<int>>> getContainer(){
        return m_container;
    }


};

#endif //MAIN_MEM_JOIN_JOINOUTPUTCOLUMNCONTAINER_H
