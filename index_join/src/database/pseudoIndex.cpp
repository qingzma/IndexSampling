//
// Created by Scott on 2019-08-20.
// Alright reserved by Qinghzi Ma
// Email: Q.Ma.2@warwick.ac.uk
// Department of Computer Science, University of Warwick,  UK
//

#include "pseudoIndex.h"
#include "../util/StringSplitter.h"

#include <iostream>
#include <regex>
#include <fstream>


PseudoIndexBuilder::PseudoIndexBuilder() {
    m_join_counts={};
    m_join_counts_acc={};
    m_sampleIndexContainer={};
}

int PseudoIndexBuilder::AppendTable(std::shared_ptr<Table> table, int RHSIndex, int LHSIndex, int tableNumber) {
    m_joinedTables.push_back(table);
    m_LHSJoinIndex.push_back(LHSIndex);
    m_RHSJoinIndex.push_back(RHSIndex);

    return int(m_joinedTables.size());
}

void PseudoIndexBuilder::Build(){
    n_tables = m_LHSJoinIndex.size();

    if (m_RHSJoinIndex.at(0) != -1 || m_LHSJoinIndex.at(n_tables-1) != -1){
        std::cout<<m_LHSJoinIndex.at(0)<<m_RHSJoinIndex.at(n_tables-1)<<std::endl;
        throw std::runtime_error("The join tables are not in the correct order!");
    }



    auto table0= m_joinedTables.begin();
    std::shared_ptr < key_index > table0Index = table0->get()->get_key_index(m_LHSJoinIndex.at(0));

    // initialize m_join_counts
    for (key_index::iterator it = table0Index->begin(); it!= table0Index->end(); it++){
        try {
            m_join_counts.at("$_"+std::to_string(it->first)) += 1;
        } catch (std::out_of_range e){
            m_join_counts.emplace("$_"+std::to_string(it->first), 1);
        }
    }

//    for (auto item: m_join_counts){
//        std::cout<<item.first<<", "<<item.second<<std::endl;
//    }

    // build path for intermediate by-directional tables.
    for (int table_count=1; table_count<n_tables-1;table_count++){
//        std::cout<<"table count is "<< table_count<<std::endl;
//        std::shared_ptr< key_index > tableIIndex = m_joinedTables.at(table_count)->get_key_index(m_RHSJoinIndex.at(table_count));
        std::shared_ptr< complex_key_index > tableIComplexIndex = m_joinedTables.at(table_count)->get_complex_key_index(m_RHSJoinIndex.at(table_count),m_LHSJoinIndex.at(table_count));
        std::shared_ptr< join_attributes_relation_index> tableIJoinIndex = m_joinedTables.at(table_count)->get_join_attribute_relation_index(m_RHSJoinIndex.at(table_count),m_LHSJoinIndex.at(table_count));

        std::map<std::string, int64_t> new_join_counts{};
        // iterate over m_join_counts
        for (auto item:m_join_counts){
//            std::cout<<"in item"<<std::endl;
            int64_t LTableLKey = StringSplitter::split_last_int64(item.first, "_");

            // if there is no join pair in the right table, delete the records in m_join_counts

            std::set<int64_t > RTableRKeys = tableIJoinIndex->at(LTableLKey);

            for (int64_t RTableRkey: RTableRKeys){
//                    std::cout<<"right label key is "<<RTableRkey<<std::endl;
                std::tuple<jfkey_t,jfkey_t> complex_key {LTableLKey, RTableRkey};
                new_join_counts[item.first+"-$_"+std::to_string(RTableRkey)]= m_join_counts[item.first] * tableIComplexIndex->count(complex_key);
            }
        }

        m_join_counts = new_join_counts;

//        for (auto item: m_join_counts){
//            std::cout<<item.first<<","<<item.second<<std::endl;
//        }
    }

    // build path for the right most table.
    std::map<std::string, int64_t> new_join_counts{};
    std::shared_ptr< key_index > tableIIndex = m_joinedTables.at(n_tables-1)->get_key_index(m_RHSJoinIndex.at(n_tables-1));
    for (auto item: m_join_counts){
        int64_t LTableLKey = StringSplitter::split_last_int64(item.first, "_");
        if (tableIIndex->count(LTableLKey) != 0){
            new_join_counts[item.first+"-$"]= m_join_counts[item.first] * tableIIndex->count(LTableLKey);
        }
    }

    m_join_counts = new_join_counts;

    m_join_counts_acc[0] = std::pair<std::string, int64_t>{"non_index", m_cadinality};
    m_cadinality = 0;
    int index = 1;
    for (auto item: m_join_counts){
        /* std::cout<<item.first<<","<<item.second<<std::endl;*/
        m_cadinality += item.second;
        m_join_counts_acc[index] = std::pair<std::string, int64_t>{item.first, m_cadinality};
        index ++;
    }

    /*for (auto value:m_join_counts_acc){
        std::cout<<value.first<<","<<value.second<<std::endl;
    }*/
};

int64_t PseudoIndexBuilder::getCardinality() {
    return m_cadinality;
}

void PseudoIndexBuilder::Sample(int sampleSize) {
    /* create samples without replacement*/
    std::set<int64_t > numbers{};
    /* Seed */
    std::random_device rd;
    /* Random number generator */
    std::default_random_engine generator(rd());
    /* Distribution on which to apply the generator */
    std::uniform_int_distribution<int64_t > distribution(1,m_cadinality);
    /* fill the set with random number. */
    for( int i=0;i <sampleSize;i++){
        numbers.insert(distribution(generator));
    }
    /* make sure the sample size is as requested.*/
    while(numbers.size()<sampleSize){
        numbers.insert(distribution(generator));
    }

    for (int64_t number:numbers){
        JoinIndexItem joinIndexItem = getJoinIndexItem(number);

        if (m_sampleIndexContainer.count(joinIndexItem.tag) ==0){
            std::vector<JoinIndexItem> item {joinIndexItem};
            m_sampleIndexContainer[joinIndexItem.tag] = item;
        }
        else{
            m_sampleIndexContainer[joinIndexItem.tag].push_back(joinIndexItem);
        }
    }

    fetchJoinTuples("pseudoSample.txt");


};


PseudoIndexBuilder::JoinIndexItem PseudoIndexBuilder::getJoinIndexItem(int64_t num) {

    /*std::cout<<num<<std::endl;*/
    int l = 0;
    int r = m_join_counts_acc.size()-1;

    int targetIndex;

    // iterative Binary Search
    while (l <= r) {
        int m = l + (r - l) / 2;

        // Check if num is present at mid
        if (std::get<1>(m_join_counts_acc[m]) >= num &&
                std::get<1>(m_join_counts_acc[m-1]) <    num){
            targetIndex = m;
            /*std::cout<<"target index is  "<<targetIndex<<std::endl;*/
            break;
        }


        // If num greater, ignore left half
        if (std::get<1>(m_join_counts_acc[m]) < num){
            l = m + 1;
            /*std::cout<<"left is changed to "<<l<<std::endl;*/
        }
            // If num is smaller, ignore right half
        else{
            r = m - 1 ;
            /*std::cout<<"right is changed to "<<r<<std::endl;*/
        }

    }
    /* std::cout<<targetIndex<<", "<<std::get<0>(m_join_counts_acc[targetIndex])<<", "<<std::get<1>(m_join_counts_acc[targetIndex])<<std::endl;*/

    std::string comIndex =  std::get<0>(m_join_counts_acc[targetIndex]);
    /*std::cout<<comIndex<<std::endl;*/
    comIndex = std::regex_replace(comIndex, std::regex("-"), "");
    comIndex = std::regex_replace(comIndex, std::regex("_"), "");
    /*std::cout<<comIndex<<std::endl;*/

    std::vector<std::string> splitter = StringSplitter::split(comIndex,"\\$");

    JoinIndexItem joinIndexItem {};
    joinIndexItem.tag = targetIndex;    /*std::get<0>(m_join_counts_acc[targetIndex]);*/
    joinIndexItem.offsetNum = std::get<1>(m_join_counts_acc[targetIndex]) - num;
    joinIndexItem.leftKey = std::stoi(splitter.at(0));
    joinIndexItem.rightKey = std::stoi(splitter.at(splitter.size()-1));

    std::vector<std::tuple<int64_t , int64_t >> midKeys{};
    for (int i =0;i < splitter.size()-1; i++){
        midKeys.push_back(std::make_tuple(std::stoi(splitter.at(i)),std::stoi(splitter.at(i+1))));
    }
    joinIndexItem.midKeys = midKeys;

    /*std::cout<<joinIndexItem.leftKey<<std::endl;
    for (auto value:midKeys){
        std::cout<<std::get<0>(value)<<","<<std::get<1>(value)<<std::endl;
    }
    std::cout<<joinIndexItem.rightKey<<std::endl;*/

    return joinIndexItem;
}


void PseudoIndexBuilder::fetchJoinTuples(std::string outfile) {
    std::ofstream output_file(outfile, std::ios::out | std::ofstream::app);


    for (auto i:m_sampleIndexContainer){
        for (JoinIndexItem joinIndexItem:i.second) {
            output_file << fetchJoinTupleRandom(joinIndexItem) << '\n';
        }
    }
    output_file.close();
}

std::string PseudoIndexBuilder::fetchJoinTupleRandom(JoinIndexItem joinIndexItem) {
    std::string outStr= "";
    /*outStr += std::to_string(joinIndexItem.leftKey);*/

    outStr += "  " + std::to_string(std::get<0>(joinIndexItem.midKeys.at(0)));
    for (auto value:joinIndexItem.midKeys){
        outStr += "  " +std::to_string(std::get<1>(value));
    }

    return outStr;
}