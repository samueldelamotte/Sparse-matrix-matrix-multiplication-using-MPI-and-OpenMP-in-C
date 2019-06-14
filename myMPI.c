#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "mpi.h"
#define MASTER 0
#define FROM_MASTER 1
#define FROM_WORKER 2

int lineCount(int i)
{
	char line[50];
	int count = 0;
	FILE *fp;
	
	if (i == 0) { //count rows file
		fp = fopen("sortedRows","r");
		if (fp == NULL) {
			perror("Could not open file.");
			exit(EXIT_FAILURE);
		}
		while (fgets(line, sizeof(line), fp) != NULL) {
			count = count + 1;
		} fclose(fp);
		return count;
	} else if (i == 1) { //count cols file
		fp = fopen("sortedCols","r");
		if (fp == NULL) {
			perror("Could not open file.");
			exit(EXIT_FAILURE);
		}
		while (fgets(line, sizeof(line), fp) != NULL) {
			count = count + 1;
		} fclose(fp);
		return count;
	}
}

void sortFile(char *file, int i)
{
	if (i == 0) { //sort by rows
		char cmd[50] = "sort -k1 -n ";
		char byRows[50] = " > sortedRows";
		strcat(cmd, file);
		strcat(cmd, byRows);
		system(cmd);
	} else if (i == 1) { //sort by cols
		char cmd[50] = "sort -k1 -n ";
		char byCols[50] = " > sortedCols";
		strcat(cmd, file);
		strcat(cmd, byCols);
		system(cmd);
	}
}

void main(int argc, char *argv[])
{
	int numtasks, taskid, numworkers, source, dest, mtype;
	MPI_Status status;
	MPI_Init(&argc, &argv);
	MPI_Comm_rank(MPI_COMM_WORLD, &taskid);
	MPI_Comm_size(MPI_COMM_WORLD, &numtasks);
	numworkers = numtasks - 1;
//----------------------------------------------------------------------------------------------------------------------------------
	if (taskid == MASTER)
	{
		/*SORT MATRIX FILES*/
		sortFile(argv[1], 0);
		sortFile(argv[2], 1);
		
		
		/*COUNT LINES IN FILES*/
		int count1; int count2;
		count1 = lineCount(0); //0 = sortedRows file
		count2 = lineCount(1); //1 = sortedCols file
		
		
		/*READ FILES, STORE IN ARRAYS*/
		int R1[count1]; int C1[count1]; double V1[count1];
		int R2[count2]; int C2[count2]; double V2[count2];
		FILE *fp1; //sortedRows file
		FILE *fp2; //sortedCols file
		char line[50]; //50 charcters should be enough
		int x = 0; //traverse array
		
		fp1 = fopen("sortedRows", "r");
		fp2 = fopen("sortedCols", "r");
		if (fp1 == NULL || fp2 == NULL) {
			perror("Could not open file.");
			exit(EXIT_FAILURE);
		}
		while (fgets(line, sizeof(line), fp1) != NULL) {
			sscanf(line, "%d %d %lf", R1+x, C1+x, V1+x);
			x = x + 1;
		} fclose(fp1);
		x = 0;
		while (fgets(line, sizeof(line), fp2) != NULL) {
			sscanf(line, "%d %d %lf", R2+x, C2+x, V2+x);
			x = x + 1;
		} fclose(fp2);
		
		
		/*SEND TO WORKERS*/
		mtype = FROM_MASTER;
		int aveData = count1/numworkers; //average sortedRows data to send
		int extraData = count1%numworkers; //extra rows data sent to first worker
		int totalData; //total data sent to worker
		int offset = 0;
		printf("Numworkers=%d\naveData=%d\nextraData=%d\n", numworkers, aveData, extraData);
		
		for (dest = 1; dest <= numworkers; dest++) {
			MPI_Send(&count2, 1, MPI_INT, dest, mtype, MPI_COMM_WORLD);
			MPI_Send(&R2, count2, MPI_INT, dest, mtype, MPI_COMM_WORLD);
			MPI_Send(&C2, count2, MPI_INT, dest, mtype, MPI_COMM_WORLD);
			MPI_Send(&V2, count2, MPI_DOUBLE, dest, mtype, MPI_COMM_WORLD);
			
			if (extraData != 0) { //there is extra data to send
				totalData = aveData + extraData;
				MPI_Send(&totalData, 1, MPI_INT, dest, mtype, MPI_COMM_WORLD);
				MPI_Send(&R1[offset], totalData, MPI_INT, dest, mtype, MPI_COMM_WORLD);
				MPI_Send(&C1[offset], totalData, MPI_INT, dest, mtype, MPI_COMM_WORLD);
				MPI_Send(&V1[offset], totalData, MPI_DOUBLE, dest, mtype, MPI_COMM_WORLD);
				offset = offset + totalData;
			} 
			if (extraData == 0) {
				totalData = aveData;
				MPI_Send(&totalData, 1, MPI_INT, dest, mtype, MPI_COMM_WORLD);
				MPI_Send(&R1[offset], totalData, MPI_INT, dest, mtype, MPI_COMM_WORLD);
				MPI_Send(&C1[offset], totalData, MPI_INT, dest, mtype, MPI_COMM_WORLD);
				MPI_Send(&V1[offset], totalData, MPI_DOUBLE, dest, mtype, MPI_COMM_WORLD);
				offset = offset + totalData;
			}
			extraData = 0; //extra has been sent to first worker so can set to 0
		}
		
		
		/*RECV FROM WORKERS*/
		mtype = FROM_WORKER;
		int *R3 = (int *)malloc(count1*count2*sizeof(int)); 
		int *C3 = (int *)malloc(count1*count2*sizeof(int)); 
		double *V3 = (double *)malloc(count1*count2*sizeof(double)); 
		totalData = 0;
		offset = 0;
		
		for (source = 1; source <= numworkers; source++) {
			MPI_Recv(&totalData, 1, MPI_INT, source, mtype, MPI_COMM_WORLD, &status);
			MPI_Recv(&R3[offset], totalData, MPI_INT, source, mtype, MPI_COMM_WORLD, &status);
			MPI_Recv(&C3[offset], totalData, MPI_INT, source, mtype, MPI_COMM_WORLD, &status);
			MPI_Recv(&V3[offset], totalData, MPI_DOUBLE, source, mtype, MPI_COMM_WORLD, &status);
			offset = offset + totalData;
		}
		

		/*WRITE DATA TO FILE*/
		FILE *fp3;
		fp3 = fopen("output.txt", "w");
		for (int i = 0; i < offset; i++) {
			fprintf(fp3, "%d %d %lf\n", R3[i], C3[i], V3[i]);
		}
		
		
//----------------------------------------------------------------------------------------------------------------------------------
	} else if (taskid > MASTER) 
	{
		/*RECV FROM MASTER*/
		mtype = FROM_MASTER;
		int count1;
		int count2;
		
		MPI_Recv(&count2, 1, MPI_INT, MASTER, mtype, MPI_COMM_WORLD, &status);
		int R2[count2]; int C2[count2]; double V2[count2];
		MPI_Recv(&R2, count2, MPI_INT, MASTER, mtype, MPI_COMM_WORLD, &status);
		MPI_Recv(&C2, count2, MPI_INT, MASTER, mtype, MPI_COMM_WORLD, &status);
		MPI_Recv(&V2, count2, MPI_DOUBLE, MASTER, mtype, MPI_COMM_WORLD, &status);
		
		MPI_Recv(&count1, 1, MPI_INT, MASTER, mtype, MPI_COMM_WORLD, &status);
		printf("Worker %d: got %d rows.\n", taskid, count1);
		int R1[count1]; int C1[count1]; double V1[count1];
		MPI_Recv(&R1, count1, MPI_INT, MASTER, mtype, MPI_COMM_WORLD, &status);
		MPI_Recv(&C1, count1, MPI_INT, MASTER, mtype, MPI_COMM_WORLD, &status);
		MPI_Recv(&V1, count1, MPI_DOUBLE, MASTER, mtype, MPI_COMM_WORLD, &status);
		
		
		/*PERFORM CALCULATION STORE INTO ARRAYS*/
		int *R3 = (int *)malloc(count1*count2*sizeof(int)); 
		int *C3 = (int *)malloc(count1*count2*sizeof(int)); 
		double *V3 = (double *)malloc(count1*count2*sizeof(double)); 
		int z = 0;
		for (int x = 0; x < count1; x++) {
			for (int y = 0; y < count2; y++) {
				if (C1[x] == R2[y]) {
					V3[z] = V1[x] * V2[y];
					R3[z] = R1[x];
					C3[z] = C2[y];
					z = z + 1;
				}
			}
		}
		
		
		/*SEND BACK TO MASTER*/
		mtype = FROM_WORKER;
		MPI_Send(&z, 1, MPI_INT, MASTER, mtype, MPI_COMM_WORLD);
		MPI_Send(&R3[0], z, MPI_INT, MASTER, mtype, MPI_COMM_WORLD);
		MPI_Send(&C3[0], z, MPI_INT, MASTER, mtype, MPI_COMM_WORLD);
		MPI_Send(&V3[0], z, MPI_DOUBLE, MASTER, mtype, MPI_COMM_WORLD);
	}
	MPI_Finalize();
}