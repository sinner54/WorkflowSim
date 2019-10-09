package org.workflowsim.examples;

import org.cloudbus.cloudsim.Cloudlet;
import org.cloudbus.cloudsim.CloudletSchedulerTimeShared;
import org.cloudbus.cloudsim.Log;
import org.cloudbus.cloudsim.Vm;
import org.workflowsim.CondorVM;
import org.workflowsim.Job;
import org.workflowsim.Task;
import org.workflowsim.WorkflowDatacenter;
import org.workflowsim.utils.Parameters;

import java.text.DecimalFormat;
import java.util.LinkedList;
import java.util.List;

/**
 * Created by Sinerous on 2019/6/26.
 */
public class WorkflowUtils {

    /**
     * 创建公有云虚拟机
     * @param userId
     * @param vms 数量
     * @return
     */
    public static List<CondorVM> createPublicVM(int userId, int vms) {

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
            vm[i] = new CondorVM(i, userId, mips * ratio, pesNumber, ram, bw, size, vmm,1,0,0,0,0.6,new CloudletSchedulerTimeShared());
            list.add(vm[i]);
        }
        for (int i = vms; i < vms * 2; i++) {
            double ratio = 2.0;
            vm[i] = new CondorVM(i, userId, mips * ratio, pesNumber, ram, 2*bw, size, vmm,1.9,0,0,0,0.8,new CloudletSchedulerTimeShared());
            list.add(vm[i]);
        }
        for (int i = 2 * vms; i < vms * 3; i++) {
            double ratio = 4.0;
            vm[i] = new CondorVM(i, userId, mips * ratio, pesNumber, ram, 4*bw, size, vmm,3.8,0,0,0,1, new CloudletSchedulerTimeShared());
            list.add(vm[i]);
        }
        return list;
    }

    /**
     * 创建私有云虚拟机
     * @param userId
     * @param vms
     * @return
     */
    public static List<CondorVM> createPrivateVM(int userId, int vms) {

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
            vm[i] = new CondorVM(i, userId, mips * ratio, pesNumber, ram, bw, size, vmm,0,0,0,0,1, new CloudletSchedulerTimeShared());
            list.add(vm[i]);
        }
        for (int i = vms; i < vms * 2; i++) {
            double ratio = 2.0;
            vm[i] = new CondorVM(i, userId, mips * ratio, pesNumber, ram, 2*bw, size, vmm,0,0,0,0,1, new CloudletSchedulerTimeShared());
            list.add(vm[i]);
        }
        return list;
    }
    /**
     *云间通信量
     * @param outputListDatecenters
     */
    public static void printInterTraffic(List<WorkflowDatacenter> outputListDatecenters) {
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
     * 打印每个虚拟机的执行时间段及费用
     * @param outputListVms
     */
    public static void printJobFT(List<? extends Vm> outputListVms) {
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
     *打印每个任务的执开始执行时间、结束时间
     * @param outputListjob
     */
    public static void printJobRFT(List<Job> outputListjob) {
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
