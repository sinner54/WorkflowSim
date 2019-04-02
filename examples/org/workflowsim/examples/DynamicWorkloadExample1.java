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

import java.io.File;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.LinkedList;
import java.util.List;

import org.cloudbus.cloudsim.*;
import org.cloudbus.cloudsim.core.CloudSim;
import org.workflowsim.*;
import org.workflowsim.utils.*;

/**
 * This DynamicWorkloadExample1 uses specifically
 * CloudletSchedulerDynamicWorkload as the local scheduler;
 *
 * @author Weiwei Chen
 * @since WorkflowSim Toolkit 1.0
 * @date Oct 13, 2013
 */
public class DynamicWorkloadExample1 extends WorkflowSimBasicExample1 {

    protected static List<CondorVM> createPublicVM(int userId, int vms) {

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
        CondorVM[] vm = new CondorVM[vms * 3];

        for (int i = 0; i < vms; i++) {
            double ratio = 1.0;
            vm[i] = new CondorVM(i, userId, mips * ratio, pesNumber, ram, bw, size, vmm,1,0,0,0,0.6,new CloudletSchedulerDynamicWorkload(mips * ratio, pesNumber));
            list.add(vm[i]);
        }
        for (int i = vms; i < vms * 2; i++) {
            double ratio = 2.0;
            vm[i] = new CondorVM(i, userId, mips * ratio, pesNumber, ram, bw, size, vmm,1.9,0,0,0,0.8,new CloudletSchedulerDynamicWorkload(mips * ratio, pesNumber));
            list.add(vm[i]);
        }
        for (int i = 2 * vms; i < vms * 3; i++) {
            double ratio = 4.0;
            vm[i] = new CondorVM(i, userId, mips * ratio, pesNumber, ram, bw, size, vmm,3.8,0,0,0,1, new CloudletSchedulerDynamicWorkload(mips * ratio, pesNumber));
            list.add(vm[i]);
        }
        return list;
    }
    protected static List<CondorVM> createPrivateVM(int userId, int vms) {

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
        CondorVM[] vm = new CondorVM[vms * 3];

        for (int i = 0; i < vms; i++) {
            double ratio = 1.0;
            vm[i] = new CondorVM(i, userId, mips * ratio, pesNumber, ram, bw, size, vmm,0,0,0,0,1, new CloudletSchedulerDynamicWorkload(mips * ratio, pesNumber));
            list.add(vm[i]);
        }
        for (int i = vms; i < vms * 2; i++) {
            double ratio = 2.0;
            vm[i] = new CondorVM(i, userId, mips * ratio, pesNumber, ram, bw, size, vmm,0,0,0,0,1, new CloudletSchedulerDynamicWorkload(mips * ratio, pesNumber));
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

            String daxPath = Parameters.daxFilePath+"Montage_1000.xml";
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
            Parameters.init(vmNum,daxPath, null,
                    null, op, cp, sch_method, pln_method,
                    null, 0);
            DistributionGenerator tempDistributionGenerator = new DistributionGenerator(DistributionGenerator.DistributionFamily.WEIBULL, 0.9d, 3);
            Parameters.setArrivalTimeModel(tempDistributionGenerator);
            ReplicaCatalog.init(file_system);

            // before creating any entities.
            int num_user = 1;   // number of grid users
            Calendar calendar = Calendar.getInstance();
            boolean trace_flag = false;  // mean trace events

            // Initialize the CloudSim library
            CloudSim.init(num_user, calendar, trace_flag);

            WorkflowDatacenter datacenter0 = createDatacenter("Datacenter_private");
            WorkflowDatacenter datacenter1 = createDatacenter("Datacenter_public");
            WorkflowDatacenter datacenter2 = createDatacenter("Datacenter_public");
            /**
             * Create a WorkflowPlanner with one schedulers.
             */
            WorkflowPlanner wfPlanner = new WorkflowPlanner("planner_0", 3);
            /**
             * Create a WorkflowEngine.
             */
            WorkflowEngine wfEngine = wfPlanner.getWorkflowEngine();
            /**
             * Create a list of VMs.The userId of a vm is basically the id of
             * the scheduler that controls this vm.
             */
            List<CondorVM> vmlist0 = createPrivateVM(wfEngine.getSchedulerId(0), 3);
            List<CondorVM> vmlist1 = createPublicVM(wfEngine.getSchedulerId(1), 4);
            List<CondorVM> vmlist2 = createPublicVM(wfEngine.getSchedulerId(2), 4);
            /**
             * Submits this list of vms to this WorkflowEngine.
             */
            wfEngine.submitVmList(vmlist0, 0);
            wfEngine.submitVmList(vmlist1,1);
            wfEngine.submitVmList(vmlist2,2);
            /**
             * Binds the data centers with the scheduler.
             */
            wfEngine.bindSchedulerDatacenter(datacenter0.getId(), 0);
            wfEngine.bindSchedulerDatacenter(datacenter1.getId(), 1);
            wfEngine.bindSchedulerDatacenter(datacenter2.getId(), 2);

            CloudSim.startSimulation();
            List<Job> outputListjob = wfEngine.getJobsReceivedList();
            List<? extends Vm> outputListVms = wfEngine.getAllVmList();
            List<WorkflowDatacenter> outputListDatecenters= new LinkedList<>();
            outputListDatecenters.add(datacenter0);
            outputListDatecenters.add(datacenter1);
            outputListDatecenters.add(datacenter2);
            CloudSim.stopSimulation();
            printJobRFT(outputListjob);
            printJobFT(outputListVms);
            printInterTraffic(outputListDatecenters);
        } catch (Exception e) {
            Log.printLine("The simulation has been terminated due to an unexpected error" );
            e.printStackTrace();
        }
    }

    /**
     *云间通信量
     * @param outputListDatecenters
     */
    private static void printInterTraffic(List<WorkflowDatacenter> outputListDatecenters) {
        String indent = "    ";
        Log.printLine();
        Log.printLine("========== OUTPUT ==========");
        DecimalFormat dft = new DecimalFormat("###.##");
        double totalInterTraffic = 0;
        Log.printLine("WorkflowDatacenter ID" + indent + "name"+ indent + indent +indent +"intertrafficSize(KB)");
        for(WorkflowDatacenter tempDatacenter:outputListDatecenters){
            Log.printLine(indent +indent +tempDatacenter.getId() + indent +indent +indent + tempDatacenter.getName()+ indent + tempDatacenter.getInterTraffic());
            totalInterTraffic += tempDatacenter.getInterTraffic();
        }
        Log.printLine("========== InterTraffic==========");
        Log.printLine("InterTraffic Num(MB):"+indent+totalInterTraffic/(1024*1024));

    }

    /**
     *
     * @param outputListVms
     */
    private static void printJobFT(List<? extends Vm> outputListVms) {
        String indent = "    ";
        Log.printLine();
        Log.printLine("========== OUTPUT ==========");

        Log.printLine("Vm ID" + indent + "startTime" + indent + "FinishTime"+indent + "costPer"+ indent + "cost");
        DecimalFormat dft = new DecimalFormat("###.##");
        double totalCost = 0;
        for (Vm vm : outputListVms) {
            CondorVM tempVm = (CondorVM)vm;
            if(tempVm.getStartTime() != Double.MAX_VALUE && tempVm.getFinishTime() != -1) {
                double cost = Math.ceil((tempVm.getFinishTime() - tempVm.getStartTime())/60) * tempVm.getCost();
                Log.printLine(vm.getUid() + indent + dft.format(tempVm.getStartTime())+
                        indent + dft.format(tempVm.getFinishTime())+
                        indent + dft.format(tempVm.getCost())+
                        indent + dft.format(cost));
                totalCost += cost;
            }
        }
        Log.printLine("========== Workflow cost==========");
        Log.printLine("Workflow public cost:"+indent+totalCost);
    }

    /**
     *
     * @param outputListjob
     */
    private static void printJobRFT(List<Job> outputListjob) {
        String indent = "    ";
        Log.printLine();
        Log.printLine("========== OUTPUT ==========");
        Log.printLine("Job ID" + indent + "Task ID" + indent + "STATUS" + indent
                + "Data center ID" + indent + "VM ID" + indent + indent
                + "Time" + indent + "Start Time" + indent + "Finish Time" + indent + "Depth");
        DecimalFormat dft = new DecimalFormat("###.##");
        double realFinishTime = -1;
        for (Job job : outputListjob) {
            Log.print(indent + job.getCloudletId() + indent + indent);
            if (job.getClassType() == Parameters.ClassType.STAGE_IN.value) {
                Log.print("Stage-in");
            }
            for (Task task : job.getTaskList()) {
                Log.print(task.getCloudletId() + ",");
            }
            Log.print(indent);

            if (job.getCloudletStatus() == Cloudlet.SUCCESS) {
                Log.print("SUCCESS");
                Log.printLine(indent + indent + job.getResourceId() + indent + indent + indent + job.getVmId()
                        + indent + indent + indent + dft.format(job.getActualCPUTime())
                        + indent + indent + dft.format(job.getExecStartTime()) + indent + indent + indent
                        + dft.format(job.getFinishTime()) + indent + indent + indent + job.getDepth());
            } else if (job.getCloudletStatus() == Cloudlet.FAILED) {
                Log.print("FAILED");
                Log.printLine(indent + indent + job.getResourceId() + indent + indent + indent + job.getVmId()
                        + indent + indent + indent + dft.format(job.getActualCPUTime())
                        + indent + indent + dft.format(job.getExecStartTime()) + indent + indent + indent
                        + dft.format(job.getFinishTime()) + indent + indent + indent + job.getDepth());
            }
            double tempjobfinishtime = job.getFinishTime();
            if (tempjobfinishtime > realFinishTime){
                realFinishTime = tempjobfinishtime;
            }
        }
        Log.printLine("========== Jobs Size=========="+outputListjob.size());
        Log.printLine("WorkflowFinishTime:"+indent+dft.format(realFinishTime));
    }
}
