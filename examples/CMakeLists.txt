add_executable(receiver_chain receiver_chain.cpp)
target_link_libraries(receiver_chain PRIVATE pipeline::pipeline)

add_executable(quickstart quickstart.cpp)
target_link_libraries(quickstart PRIVATE pipeline::pipeline)

add_executable(multichannel_receiver multichannel_receiver.cpp)
target_link_libraries(multichannel_receiver PRIVATE pipeline::pipeline)
