/**
 * Copyright 2012-2013 University Of Southern California
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package org.workflowsim.planning;

import org.cloudbus.cloudsim.Consts;
import org.cloudbus.cloudsim.Log;
import org.cloudbus.cloudsim.core.CloudSim;
import org.workflowsim.CondorVM;
import org.workflowsim.FileItem;
import org.workflowsim.Task;
import org.workflowsim.utils.Parameters;

import java.util.*;

/**
 * The PCH planning algorithm.
 *
 * @author Pedro Paulo Vezzá Campos
 * @date Oct 12, 2013
 */
public class PCHPlanningAlgorithm extends BasePlanningAlgorithm {

    private Map<Task, Map<CondorVM, Double>> computationCosts;
    private Map<Task, Map<Task, Double>> transferCosts;
    private Map<Task, Double> rank;
    private Map<CondorVM, List<Event>> schedules;
    private Map<Task, Double> earliestFinishTimes;
    private double maxBandwidth;//虚拟机间最大带宽，计算过程中再改变
    private List<TaskRank> unscheduleTasks;


    public PCHPlanningAlgorithm() {
        computationCosts = new HashMap<>();
        transferCosts = new HashMap<>();
        rank = new HashMap<>();
        earliestFinishTimes = new HashMap<>();
        schedules = new HashMap<>();
        unscheduleTasks = new LinkedList();
    }

    /**
     * The main function
     */
    @Override
    public void run() {
        Log.printLine("PCH planner running with " + getTaskList().size()  + " tasks.");

        maxBandwidth = calculateMaxBandwidth();

        for (Object vmObject : getVmList()) {
            CondorVM vm = (CondorVM) vmObject;
            schedules.put(vm, new ArrayList<Event>());
        }

        // Prioritization phase
        calculateComputationCosts();
        calculateTransferCosts();
        calculateRanks();

        //初始化
        unscheduleTasks = new ArrayList<>();
        for (Task task : rank.keySet()) {
            unscheduleTasks.add(new TaskRank(task, rank.get(task)));
        }
        // Sorting in non-ascending order of rank
        Collections.sort(unscheduleTasks);

        // Selection phase
        while(!unscheduleTasks.isEmpty()){
            List<Task> clustertasks = get_next_cluster();
            CondorVM bestvm = get_best_resource(clustertasks);
            allocateTask(bestvm,clustertasks);
        }

    }

    /**
     * 获得使得clusterTask的后继任务最小
     * @param clustertasks
     * @return
     */
    private CondorVM get_best_resource(List<Task> clustertasks) {
        double earliestEST = Double.MAX_VALUE;
        CondorVM bestVm = null;
        for(CondorVM tempVm :schedules.keySet()){
            double tempEST = 0;
            //找到使得cluster的后继EST最小的虚拟机资源
            for(Task tempTask :clustertasks){
                //临时调度，不能修改调度里的值
                //z
                double minReadyTime = 0;
                for (Task parent : tempTask.getParentList()) {
                    double readyTime = earliestFinishTimes.get(parent);
                    if (parent.getVmId() != tempVm.getId()) {
                        readyTime += transferCosts.get(parent).get(tempTask);
                    }
                    minReadyTime = Math.max(minReadyTime, readyTime);
                }
                double finishTime = findFinishTime(tempTask, tempVm, minReadyTime, false);
            }
            if (tempEST < earliestEST) {
                earliestEST = tempEST;
                bestVm = tempVm;
            }
        }
        return bestVm;
    }

    /**
     *获得cluster
     * @return
     */
    private List<Task> get_next_cluster() {
        List<Task> cluster = new ArrayList<>();
        Task tempUnscheduledWithHP = unscheduleTasks.get(0).task;
        cluster.add(tempUnscheduledWithHP);
        while(HasUnscheduledSucc(tempUnscheduledWithHP)){
            //highest Pi + EST的后继任务
            double maxPi_EST = 0;
            Task bestSuccTask = null;
            List<Task> childTask = tempUnscheduledWithHP.getChildList();
            if (childTask!= null && childTask.size() != 0){
                for(Task tempTask:childTask){
                    double tempPi_EST = rank.get(tempTask)+earliestFinishTimes.get(tempTask);
                    if (tempPi_EST > maxPi_EST){
                        maxPi_EST = tempPi_EST;
                        bestSuccTask = tempTask;
                    }
                }
            }
            cluster.add(bestSuccTask);
            tempUnscheduledWithHP = bestSuccTask;
        }
        return cluster;
    }

    /**
     * 判断是否有未调度的后继任务
     * @param tempUnscheduledWithHP
     * @return
     */
    private boolean HasUnscheduledSucc(Task tempUnscheduledWithHP) {
        boolean hasUnscheduleTask = false;
        if (tempUnscheduledWithHP != null){
            List<Task> childTask = tempUnscheduledWithHP.getChildList();
            if (childTask!= null && childTask.size() != 0){
                for(Task tempTask:childTask){
                    if(unscheduleTasks.contains(tempTask)){
                        hasUnscheduleTask = true;
                        break;
                    }
                }
            }
        }
        return hasUnscheduleTask;
    }

    /**
     * 获得未调度中优先级最大的任务
     * @return
     */
    private Task getUnscheduledTaskWithhighestPriority() {
        unscheduleTasks.get(0);
    }

    /**
     * Calculates the max available bandwidth among all VMs in Mbit/s
     *
     * @return Average available bandwidth in Mbit/s
     */
    private double calculateMaxBandwidth() {
        double max = 0.0;
        for (Object vmObject : getVmList()) {
            CondorVM vm = (CondorVM) vmObject;
            if(vm.getBw() > max) {
             max = vm.getBw();
            }
        }
        return max;
    }

    /**
     * Populates the computationCosts field with the time in seconds to compute
     * a task in a vm.
     */
    private void calculateComputationCosts() {
        for (Task task : getTaskList()) {
            Map<CondorVM, Double> costsVm = new HashMap<>();
            for (Object vmObject : getVmList()) {
                CondorVM vm = (CondorVM) vmObject;
                if (vm.getNumberOfPes() < task.getNumberOfPes()) {
                    costsVm.put(vm, Double.MAX_VALUE);
                } else {
                    costsVm.put(vm,
                            task.getCloudletTotalLength() / vm.getMips());
                }
            }
            computationCosts.put(task, costsVm);
        }
    }

    /**
     * Populates the transferCosts map with the time in seconds to transfer all
     * files from each parent to each child
     */
    private void calculateTransferCosts() {
        // Initializing the matrix
        for (Task task1 : getTaskList()) {
            Map<Task, Double> taskTransferCosts = new HashMap<>();
            for (Task task2 : getTaskList()) {
                taskTransferCosts.put(task2, 0.0);
            }
            transferCosts.put(task1, taskTransferCosts);
        }

        // Calculating the actual values
        for (Task parent : getTaskList()) {
            for (Task child : parent.getChildList()) {
                transferCosts.get(parent).put(child,
                        calculateTransferCost(parent, child));
            }
        }
    }

    /**
     * Accounts the time in seconds necessary to transfer all files described
     * between parent and child
     *
     * @param parent
     * @param child
     * @return Transfer cost in seconds
     */
    private double calculateTransferCost(Task parent, Task child) {
        List<FileItem> parentFiles = parent.getFileList();
        List<FileItem> childFiles = child.getFileList();

        double acc = 0.0;

        for (FileItem parentFile : parentFiles) {
            if (parentFile.getType() != Parameters.FileType.OUTPUT) {
                continue;
            }

            for (FileItem childFile : childFiles) {
                if (childFile.getType() == Parameters.FileType.INPUT
                        && childFile.getName().equals(parentFile.getName())) {
                    acc += childFile.getSize();
                    break;
                }
            }
        }

        //file Size is in Bytes, acc in MB
        acc = acc / Consts.MILLION;
        // acc in MB, averageBandwidth in Mb/s
        return acc * 8 / maxBandwidth;
    }

    /**
     * Invokes calculateRank for each task to be scheduled
     */
    private void calculateRanks() {
        for (Task task : getTaskList()) {
            calculateRank(task);
        }
    }

    /**
     * Populates rank.get(task) with the rank of task as defined in the HEFT
     * paper.
     *
     * @param task The task have the rank calculates
     * @return The rank
     */
    private double calculateRank(Task task) {
        if (rank.containsKey(task)) {
            return rank.get(task);
        }

        double minComputationCost = Double.MAX_VALUE;//使用性能最好的虚拟机来处理

        for (Double cost : computationCosts.get(task).values()) {
            if (cost < minComputationCost){
                minComputationCost = cost;
            }
        }
        double max = 0.0;
        for (Task child : task.getChildList()) {
            double childCost = transferCosts.get(task).get(child)
                    + calculateRank(child);
            max = Math.max(max, childCost);
        }
        rank.put(task, minComputationCost + max);
        return rank.get(task);
    }

    /**
     *
     * @param bestvm
     * @param clustertasks
     */
    private void allocateTask(CondorVM bestvm, List<Task> clustertasks) {

        for(Task tempTask:clustertasks) {
//            findFinishTime(tempTask, bestvm, bestReadyTime, true);//这部分调整到查找最优资源那里去
//            earliestFinishTimes.put(tempTask, earliestFinishTime);//这部分调整到查找最优资源那里去
            tempTask.setVmId(bestvm.getId());
            tempTask.setUserId(bestvm.getUserId());//调度到对应的scheduler
            Log.printLine(Parameters.df.format(CloudSim.clock()) + " :Planning " + tempTask.getCloudletId() + " with  "
                    + tempTask.getCloudletLength() + "  &arrivaleTime: " + Parameters.df.format(tempTask.getArrivalTime())
                    + " to Scheduler " + tempTask.getUserId()
                    + " VM " + tempTask.getVmId());
        }
    }

    /**
     * Finds the best time slot available to minimize the finish time of the
     * given task in the vm with the constraint of not scheduling it before
     * readyTime. If occupySlot is true, reserves the time slot in the schedule.
     *
     * @param task The task to have the time slot reserved
     * @param vm The vm that will execute the task
     * @param readyTime The first moment that the task is available to be
     * scheduled
     * @param occupySlot If true, reserves the time slot in the schedule.
     * @return The minimal finish time of the task in the vmn
     */
    private double findFinishTime(Task task, CondorVM vm, double readyTime,
            boolean occupySlot) {
        List<Event> sched = schedules.get(vm);
        double computationCost = computationCosts.get(task).get(vm);
        double start, finish;
        int pos;

        if (sched.isEmpty()) {
            if (occupySlot) {
                sched.add(new Event(readyTime, readyTime + computationCost));
            }
            return readyTime + computationCost;
        }

        if (sched.size() == 1) {
            if (readyTime >= sched.get(0).finish) {
                pos = 1;
                start = readyTime;
            } else if (readyTime + computationCost <= sched.get(0).start) {
                pos = 0;
                start = readyTime;
            } else {
                pos = 1;
                start = sched.get(0).finish;
            }

            if (occupySlot) {
                sched.add(pos, new Event(start, start + computationCost));
            }
            return start + computationCost;
        }

        // Trivial case: Start after the latest task scheduled
        start = Math.max(readyTime, sched.get(sched.size() - 1).finish);
        finish = start + computationCost;
        int i = sched.size() - 1;
        int j = sched.size() - 2;
        pos = i + 1;
        while (j >= 0) {
            Event current = sched.get(i);
            Event previous = sched.get(j);

            if (readyTime > previous.finish) {
                if (readyTime + computationCost <= current.start) {
                    start = readyTime;
                    finish = readyTime + computationCost;
                }

                break;
            }
            if (previous.finish + computationCost <= current.start) {
                start = previous.finish;
                finish = previous.finish + computationCost;
                pos = i;
            }
            i--;
            j--;
        }

        if (readyTime + computationCost <= sched.get(0).start) {
            pos = 0;
            start = readyTime;

            if (occupySlot) {
                sched.add(pos, new Event(start, start + computationCost));
            }
            return start + computationCost;
        }
        if (occupySlot) {
            sched.add(pos, new Event(start, finish));
        }
        return finish;
    }
    private class Event {

        public double start;
        public double finish;

        public Event(double start, double finish) {
            this.start = start;
            this.finish = finish;
        }
    }

    private class TaskRank implements Comparable<TaskRank> {

        public Task task;
        public Double rank;

        public TaskRank(Task task, Double rank) {
            this.task = task;
            this.rank = rank;
        }

        @Override
        public int compareTo(TaskRank o) {
            return o.rank.compareTo(rank);
        }
    }
}
