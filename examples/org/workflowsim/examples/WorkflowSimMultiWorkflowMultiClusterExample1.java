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
package org.workflowsim.examples;

import org.cloudbus.cloudsim.*;
import org.cloudbus.cloudsim.core.CloudSim;
import org.cloudbus.cloudsim.provisioners.BwProvisionerSimple;
import org.cloudbus.cloudsim.provisioners.PeProvisionerSimple;
import org.cloudbus.cloudsim.provisioners.RamProvisionerSimple;
import org.workflowsim.*;
import org.workflowsim.utils.*;

import java.io.File;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.LinkedList;
import java.util.List;

/**
 * This DynamicWorkloadExample1 uses specifically
 * CloudletSchedulerDynamicWorkload as the local scheduler;
 *
 * @author Weiwei Chen
 * @since WorkflowSim Toolkit 1.0
 * @date Oct 13, 2013
 */
public class WorkflowSimMultiWorkflowMultiClusterExample1 extends WorkflowSimBasicExample1 {

    /**
     * 创建VMs
     * @param userId
     * @param vms
     * @return
     */
    protected static List<CondorVM> createVM(int userId, int vms) {

        //Creates a container to store VMs. This list is passed to the broker later
        LinkedList<CondorVM> list = new LinkedList<>();

        //VM Parameters
        long size = 10000; //image size (MB)
        int ram = 512; //vm memory (MB)
        int mips = 1000;
        long bw = 1000;
        int pesNumber = 1; //number of cpus
        String vmm = "Xen"; //VMM name

        //create VMs
        CondorVM[] vm = new CondorVM[vms];

        for (int i = 0; i < vms; i++) {
            double ratio = 1.0;
            vm[i] = new CondorVM(i, userId, mips * ratio, pesNumber, ram, bw, size, vmm, new CloudletSchedulerDynamicWorkload(mips * ratio, pesNumber));
            list.add(vm[i]);
        }

        return list;
    }

    ////////////////////////// STATIC METHODS ///////////////////////
    /**
     * Creates main() to run this example This example has only one datacenter
     * and one storage
     */
    public static void main(String[] args) {

        try {
            // First step: Initialize the WorkflowSim package. 

            /**
             * However, the exact number of vms may not necessarily be vmNum If
             * the data center or the host doesn't have sufficient resources the
             * exact vmNum would be smaller than that. Take care.
             */
            int vmNum = 20;//number of vms;
            /**
             * Should change this based on real physical path
             */
            List<String> daxPaths = new ArrayList<>();

            /**
             * 添加多工作流
             */
            String daxPath = Parameters.daxFilePath+"Montage_25.xml";
            File daxFile = new File(daxPath);
            if (!daxFile.exists()) {
                Log.printLine("Warning: Please replace daxPath with the physical path in your working environment!");
                return;
            }
            daxPaths.add(daxPath);
            daxPaths.add(daxPath);
            daxPaths.add(daxPath);
            /**
             * Since we are using HEFT planning algorithm, the scheduling
             * algorithm should be static such that the scheduler would not
             * override the result of the planner
             */
            Parameters.SchedulingAlgorithm sch_method = Parameters.SchedulingAlgorithm.STATIC;
            Parameters.PlanningAlgorithm pln_method = Parameters.PlanningAlgorithm.HEFT;
            ReplicaCatalog.FileSystem file_system = ReplicaCatalog.FileSystem.LOCAL;

            /**
             * No overheads
             */
            OverheadParameters op = new OverheadParameters(0, null, null, null, null, 0);

            /**
             * No Clustering
             */
            ClusteringParameters.ClusteringMethod method = ClusteringParameters.ClusteringMethod.NONE;
            ClusteringParameters cp = new ClusteringParameters(0, 0, method, null);

            /**
             * Initialize static parameters
             */
            Parameters.init(vmNum, daxPaths, null,
                    null, op, cp, sch_method, pln_method,
                    null, 0);
            /**
             * 任务达到时间服从分布
             */
            DistributionGenerator tempDistributionGenerator = new DistributionGenerator(DistributionGenerator.DistributionFamily.WEIBULL, 0.9d, 3);
            Parameters.setArrivalTimeModel(tempDistributionGenerator);
            ReplicaCatalog.init(file_system);

            // before creating any entities.
            int num_user = 1;   // number of grid users
            Calendar calendar = Calendar.getInstance();
            boolean trace_flag = false;  // mean trace events

            // Initialize the CloudSim library
            CloudSim.init(num_user, calendar, trace_flag);
            /**
             * Create a WorkflowPlanner with one schedulers.
             */
            WorkflowPlanner wfPlanner = new WorkflowPlanner("planner_0", 1);
            /**
             * Create a WorkflowEngine.
             */
            WorkflowEngine wfEngine = wfPlanner.getWorkflowEngine();
            //创建多个数据中心
            int maxDatacenterId = 3;
            for(int i = 0; i < maxDatacenterId; i++ ){
                WorkflowDatacenter datacenter0 = createDatacenter(i,maxDatacenterId);
                /**
                 * Create a list of VMs.The userId of a vm is basically the id of
                 * the scheduler that controls this vm.
                 */
                List<CondorVM> vmlist0 = createVM(wfEngine.getSchedulerId(0), Parameters.getVmNum());
                /**
                 * Submits this list of vms to this WorkflowEngine.
                 */
                wfEngine.submitVmList(vmlist0, 0);

                /**
                 * Binds the data centers with the scheduler.
                 */
                wfEngine.bindSchedulerDatacenter(datacenter0.getId(), 0);
            }

            CloudSim.startSimulation();
            List<Job> outputList0 = wfEngine.getJobsReceivedList();
            CloudSim.stopSimulation();
            printJobList(outputList0);
        } catch (Exception e) {
            Log.printLine("The simulation has been terminated due to an unexpected error");
        }
    }
    protected static WorkflowDatacenter createDatacenter(int dataCenterId,int maxDataCenterId) {

        // Here are the steps needed to create a PowerDatacenter:
        // 1. We need to create a list to store one or more
        //    Machines
        List<Host> hostList = new ArrayList<>();

        // 2. A Machine contains one or more PEs or CPUs/Cores. Therefore, should
        //    create a list to store these PEs before creating
        //    a Machine.
        //
        // Here is the trick to use multiple data centers in one broker. Broker will first
        // allocate all vms to the first datacenter and if there is no enough resource then it will allocate
        // the failed vms to the next available datacenter. The trick is make sure your datacenter is not
        // very big so that the broker will distribute them.
        // In a future work, vm scheduling algorithms should be done
        //
        for (int i = 1; i <= 3; i++) {
            List<Pe> peList1 = new ArrayList<>();
            int mips = 2000;
            // 3. Create PEs and add these into the list.
            //for a quad-core machine, a list of 4 PEs is required:
            peList1.add(new Pe(0, new PeProvisionerSimple(mips))); // need to store Pe id and MIPS Rating
            peList1.add(new Pe(1, new PeProvisionerSimple(mips)));
            peList1.add(new Pe(2, new PeProvisionerSimple(mips)));
            peList1.add(new Pe(3, new PeProvisionerSimple(mips)));
            int hostId = 0;
            int ram = 2048 * 16 ; //host memory (MB) 32GB
            long storage = 1000000; //host storage
            int bw = 10000;
            hostList.add(
                    new Host(
                            hostId,
                            new RamProvisionerSimple(ram),
                            new BwProvisionerSimple(bw),
                            storage,
                            peList1,
                            new VmSchedulerTimeShared(peList1))); // This is our first machine
            hostId++;
        }

        // 5. Create a DatacenterCharacteristics object that stores the
        //    properties of a data center: architecture, OS, list of
        //    Machines, allocation policy: time- or space-shared, time zone
        //    and its price (G$/Pe time unit).
        String arch = "x86";      // system architecture
        String os = "Linux";          // operating system
        String vmm = "Xen";
        /**
         * 实际的云提供商不会如此收费，
         * 针对VM，Amazon,阿里云会按照VM的性能类型来按计费周期进行收费。按量付费
         * 针对存储收费，按照消耗的存储空间来收取费用；
         * 存储费用是根据你所占用的存储空间大小计费，没有购买包年包月资源包的用户，默认按【按量付费】进行扣费。
         * 0-5GB(含)部分免费，5GB以上部分0.136元/GB/月；
         * 针对网络流量，阿里云
         */

        double time_zone = 10.0;         // time zone this resource located
        double cost = 3.0;              // the cost of using processing in this resource
        double costPerMem = 0.05;		// the cost of using memory in this resource
        double costPerStorage = 0.1;	// the cost of using storage in this resource
        double costPerBw = 0.1;			// the cost of using bw in this resource
        LinkedList<Storage> storageList = new LinkedList<>();	//we are not adding SAN devices by now
        WorkflowDatacenter datacenter = null;
        DatacenterCharacteristics characteristics = new DatacenterCharacteristics(
                arch, os, vmm, hostList, time_zone, cost, costPerMem, costPerStorage, costPerBw);

        // 6. Finally, we need to create a cluster storage object.
        /**
         * The bandwidth between data centers.
         */
        double interBandwidth = 1.5e7;// the number comes from the futuregrid site, you can specify your bw
        /**
         * The bandwidth within a data center.
         */
        double intraBandwidth = interBandwidth * 2;
        try {
            ClusterStorage s1 = new ClusterStorage("Datacenter_"+dataCenterId, 1e12);
            //内部带宽是到其他的数据中心的带宽的2倍
            for(int i =0; i < maxDataCenterId;i++ ){
                if (dataCenterId !=  i ){
                    s1.setBandwidth("Datacenter_"+i, interBandwidth);
                }
            }
            // The bandwidth within a data center
            s1.setBandwidth("local", intraBandwidth);
            // The bandwidth to the source site
            s1.setBandwidth("source", interBandwidth);
            storageList.add(s1);
            datacenter = new WorkflowDatacenter("Datacenter_"+dataCenterId, characteristics, new VmAllocationPolicySimple(hostList), storageList, 0);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return datacenter;
    }
}
