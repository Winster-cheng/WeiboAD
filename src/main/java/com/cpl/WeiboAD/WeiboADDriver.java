package com.cpl.WeiboAD;

import java.io.IOException;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;


public class WeiboADDriver {
		public static void main(String[] args) throws Exception {
			Job job1 = CalcTFAndN.getJobTFAndN();
			Job job2 = CalcDF.getJobCalcN();
			Job job3=CalcALL.getJobCalcAll();
			ControlledJob controlledJob1 = new ControlledJob(
					job1.getConfiguration());
			controlledJob1.setJob(job1);

			ControlledJob controlledJob2 = new ControlledJob(
					job2.getConfiguration());
			controlledJob2.setJob(job2);
			
			ControlledJob controlledJob3 = new ControlledJob(
					job3.getConfiguration());
			controlledJob3.setJob(job3);
			
			controlledJob2.addDependingJob(controlledJob1);
			controlledJob3.addDependingJob(controlledJob2);
			
			JobControl jc = new JobControl("weiboad job control");
			jc.addJob(controlledJob1);
			jc.addJob(controlledJob2);
			jc.addJob(controlledJob3);
			Thread jcThread = new Thread(jc);
			jcThread.start();
			while (true) {
				if (jc.allFinished()) {
					System.out.println(jc.getSuccessfulJobList());
					jc.stop();
					break;
				}
				if (jc.getFailedJobList().size() > 0) {
					System.out.println(jc.getFailedJobList());
					jc.stop();
					break;
				}
			}
		}
}
