#include "sorter_thread.h"
#define N 1024  //size of the buffer

struct arg_struct {
    char  f[1024];
    int  b[1024];
}args;

int sizeOfStruct;
int count = 0 ;//totoal file counter 
int csvCount = 0 ;//csv file counter
int threadCount = 1;//thread counter
int numCol;
char  dirpath[1024]={"."};
char col[40];
char outputdir[1024]={"."};

void mergeSort(struct proj * arr , int l, int r, int colNum );
void merge (struct proj * instance1,int fL, int fR, int sL, int sR, int colNum );
void printEverything (struct proj* instance1, int elementNo2,char filename[1024]);
//void *readFileData(char*,char*);
void *readFileData(void *);


 void mergeintoone(){
    DIR *dir;
    pthread_t ttid;
    struct dirent *entry;
    FILE *fp,*fp1;
    char data[33];
    char path[1024];
    char readfile[1024];
    //char masterfile[1024];
    char outputstr[1024];
    //char* outputstr = "AllFiles-sorted-";
    int ctr;
    printf("\noutput dir = %s",outputdir);
    if (!(dir = opendir(outputdir)))
        return;
    strcpy(outputstr,""); 
    strcat(outputstr,"AllFiles-sorted-");
    strcat(outputstr,col);
    strcat(outputstr,".csv");
    

	
    printf("\ndata write into master => %s",outputstr);
    
    fp1 = fopen(outputstr,"w");
    fprintf(fp1,"%s, %s, %s, %s, %s, %s, %s, %s, %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s", 
    "color","director_name","num_critic_for_reviews","duration",
    "director_facebook_likes","actor_3_facebook_likes","actor_2_name",
    "actor_1_facebook_likes","gross","genres","actor_1_name",
    "movie_title","num_voted_users","cast_total_facebook_likes","actor_3_name", 
     "facenumber_in_poster", "plot_keywords", "movie_imdb_link", "num_user_for_reviews",
     "language", "country", "content_rating", "budget",
     "title_year","actor_2_facebook_likes", "imdb_score","aspect_ratio",
     "movie_facebook_likes");
     
    while ((entry = readdir(dir)) != NULL) {
                strcpy(readfile,"");
                strcat(readfile,outputdir);
                strcat(readfile,"/");
                strcat(readfile,entry->d_name);
            if(strstr(readfile,".csv") && !strstr(readfile,"allsort.csv") ){
                 
                printf("\nMerging = %s",readfile);
                fp = fopen(readfile,"r");                
                                struct proj * instance1 = (struct proj *)malloc(sizeof(struct proj)*35000); 
    
                                
                                 int columnCounter, elementNo,buffSize; 
                                 char buffer[N];


                                 columnCounter = 0;
                                 elementNo = 0;
                                 buffSize = sizeof(buffer);
                                //fgets(buffer, buffSize, fp);
                                    while(fgets(buffer, buffSize, fp) != NULL)
                                    {
                                         int i = 0; 
                                        int columnToSort = 0;
                                        
                                        int counterForCommas = 1;
                                    while(buffer[counterForCommas] != '\0')
                                    {
                                        if(buffer[counterForCommas] == ',')
                                        {
                                            if(buffer[counterForCommas - 1] == ',')
                                            {
                                                char cma = buffer[counterForCommas];  //comma = cma 
                                                buffer[counterForCommas] = 'x';
                                                int newCounter = 1;

                                                        for(newCounter; buffer[counterForCommas + newCounter] < (1024- (counterForCommas + newCounter)); newCounter++)
                                                        {
                                                            char newChar = buffer[counterForCommas + newCounter];
                                                            buffer[counterForCommas + newCounter] = cma;
                                                            cma = newChar;
                                                            int counterForCommas2 = 0;
                                                        }   
                                                    }
                                            }
                                        counterForCommas++;
                                    
                                        }



                                    int xtemp,ytemp = 0;
                                    for(xtemp = 0; buffer[xtemp] != '\0'; xtemp++)
                                    {
                                        if(buffer[xtemp]=='"')
                                        {
                                            for(ytemp = (xtemp+1); buffer[ytemp] != '"'; ytemp++)
                                            {
                                                if(buffer[ytemp]==',')
                                                {
                                                    buffer[ytemp]='^';
                                                }
                                            }
                                            xtemp = ytemp + 1;
                                        }
                                    }





                                        char * tokenizedStr = strtok(buffer, ",");
                                        int qCnt; //quoteCount
                                
                                        if(buffer[0] == ',')
                                        {  
                                            char * tok2 = "no input";
                                            instance1[elementNo].color = (char *)malloc(sizeof(struct proj));
                                            strcpy(instance1[elementNo].color, tok2);
                                            columnToSort++;
                                        }
                                
                                        while(tokenizedStr != NULL)
                                        {
                                            int lengthQuote = strlen(tokenizedStr);
                                            for(qCnt = 0; qCnt < lengthQuote; qCnt++)
                                            {
                                                    if(tokenizedStr[qCnt] == '"')
                                                    {
                                                        int p = 0;
                                                        for(p; p < lengthQuote; p++)//
                                           
                                                            if(tokenizedStr[p] == '^')
                                                            {
                                                                    tokenizedStr[p] = ',';
                                                            }
                                                    }
                                        }   
                                        switch(columnToSort)
                                        {

                                    case 0 :    instance1[elementNo].color = (char *)malloc(sizeof(struct proj));
                                                    strcpy(instance1[elementNo].color, tokenizedStr);
                                                    tokenizedStr = strtok(NULL, ",");
                                            break;

                                    case 1 :    instance1[elementNo].director_name = (char *)malloc(sizeof(struct proj));
                                                    strcpy(instance1[elementNo].director_name, tokenizedStr);
                                                    tokenizedStr = strtok(NULL, ",");
                                            break;

                                    case 2 :    instance1[elementNo].num_critic_for_reviews = atoi(tokenizedStr);
                                                    tokenizedStr = strtok(NULL, ",");
                                            break;

                                    case 3 :    instance1[elementNo].duration = atoi(tokenizedStr);
                                                    tokenizedStr = strtok(NULL, ",");
                                            break;

                                    case 4 :    instance1[elementNo].director_facebook_likes = atoi(tokenizedStr);
                                                    tokenizedStr = strtok(NULL, ",");
                                            break;

                                    case 5 :    instance1[elementNo].actor_3_facebook_likes = atoi(tokenizedStr);
                                                    tokenizedStr = strtok(NULL, ",");
                                            break;

                                    case 6 :    instance1[elementNo].actor_2_name = (char *)malloc(sizeof(struct proj));
                                                    strcpy(instance1[elementNo].actor_2_name, tokenizedStr);
                                                    tokenizedStr = strtok(NULL, ",");
                                            break;

                                    case 7 :    instance1[elementNo].actor_1_facebook_likes = atoi(tokenizedStr);
                                                    tokenizedStr = strtok(NULL, ",");
                                            break;

                                    case 8 :    instance1[elementNo].gross = atoi(tokenizedStr);
                                                    tokenizedStr = strtok(NULL, ",");
                                            break;

                                    case 9 :    instance1[elementNo].genres = (char *)malloc(sizeof(struct proj));
                                                    strcpy(instance1[elementNo].genres, tokenizedStr);
                                                    tokenizedStr = strtok(NULL, ",");
                                            break;

                                    case 10 :   instance1[elementNo].actor_1_name = (char *)malloc(sizeof(struct proj));
                                                    strcpy(instance1[elementNo].actor_1_name, tokenizedStr);
                                                    tokenizedStr = strtok(NULL, ",");
                                            break;

                                    case 11 :   instance1[elementNo].movie_title = (char *)malloc(sizeof(struct proj));
                                                    strcpy(instance1[elementNo].movie_title, tokenizedStr);
                                                    tokenizedStr = strtok(NULL, ",");
                                            break;

                                    case 12 :   instance1[elementNo].num_voted_users = atoi(tokenizedStr);
                                                    tokenizedStr = strtok(NULL, ",");
                                            break;

                                    case 13 :   instance1[elementNo].cast_total_facebook_likes = atoi(tokenizedStr);
                                                    tokenizedStr = strtok(NULL, ",");
                                            break;

                                    case 14 :   instance1[elementNo].actor_3_name = (char *)malloc(sizeof(struct proj));
                                                    strcpy(instance1[elementNo].actor_3_name, tokenizedStr);
                                                    tokenizedStr = strtok(NULL, ",");
                                            break;

                                    case 15 :   instance1[elementNo].facenumber_in_poster = atoi(tokenizedStr);
                                                    tokenizedStr = strtok(NULL, ",");
                                            break;

                                    case 16 :   instance1[elementNo].plot_keywords = (char *)malloc(sizeof(struct proj));
                                                    strcpy(instance1[elementNo].plot_keywords, tokenizedStr);
                                                    tokenizedStr = strtok(NULL, ",");
                                            break;

                                    case 17 :   instance1[elementNo].movie_imdb_link = (char *)malloc(sizeof(struct proj));
                                                    strcpy(instance1[elementNo].movie_imdb_link, tokenizedStr);
                                                    tokenizedStr = strtok(NULL, ",");
                                            break;

                                    case 18 :   instance1[elementNo].num_user_for_reviews = atoi(tokenizedStr);
                                                    tokenizedStr = strtok(NULL, ",");
                                            break;

                                    case 19 :   instance1[elementNo].language = (char *) malloc(sizeof(struct proj));
                                                    strcpy(instance1[elementNo].language, tokenizedStr);
                                                    tokenizedStr = strtok(NULL, ",");
                                            break;

                                    case 20 :   instance1[elementNo].country = (char *) malloc(sizeof(struct proj));
                                                    strcpy(instance1[elementNo].country, tokenizedStr);
                                                    tokenizedStr = strtok(NULL, ",");
                                            break;

                                    case 21:    instance1[elementNo].content_rating = (char *) malloc(sizeof(struct proj));
                                                    strcpy(instance1[elementNo].content_rating, tokenizedStr);
                                                    tokenizedStr = strtok(NULL, ",");
                                            break;

                                    case 22 :   instance1[elementNo].budget = atoi(tokenizedStr);
                                                    tokenizedStr = strtok(NULL, ",");
                                            break;

                                    case 23 :   instance1[elementNo].title_year = atoi(tokenizedStr);
                                                    tokenizedStr = strtok(NULL, ",");
                                            break;

                                    case 24 :   instance1[elementNo].actor_2_facebook_likes = atoi(tokenizedStr);
                                                    tokenizedStr = strtok(NULL, ",");
                                            break;

                                    case 25 :   instance1[elementNo].imdb_score = atof(tokenizedStr);
                                                    tokenizedStr = strtok(NULL, ",");
                                            break;

                                    case 26 :   instance1[elementNo].aspect_ratio = atof(tokenizedStr);
                                                    tokenizedStr = strtok(NULL, ",");
                                            break;

                                    case 27 :   instance1[elementNo].movie_facebook_likes = atoi(tokenizedStr);
                                                    tokenizedStr = strtok(NULL, ",");
                                            break;

                                    default :       break;

                                    }

                                    

                                        columnToSort++;
                                        
                                        }
                                    elementNo++; //increments couter for instance1
                                }
                         fclose(fp);
                         for(ctr = 1; ctr < elementNo; ctr++)
                           {
                           fprintf(fp1,"\n%s, %s, %d, %d, %d, %d, %s, %d, %d,%s,%s,%s,%d,%d,%s,%d,%s,%s,%d,%s,%s,%s,%d,%d,%d,%f,%f,%d", 
                            instance1[ctr].color,instance1[ctr].director_name,instance1[ctr].num_critic_for_reviews,instance1[ctr].duration,
                            instance1[ctr].director_facebook_likes,instance1[ctr].actor_3_facebook_likes,instance1[ctr].actor_2_name,
                            instance1[ctr].actor_1_facebook_likes, instance1[ctr].gross, instance1[ctr].genres,instance1[ctr].actor_1_name,
                            instance1[ctr].movie_title,instance1[ctr].num_voted_users, instance1[ctr].cast_total_facebook_likes, instance1[ctr].actor_3_name, 
                            instance1[ctr].facenumber_in_poster, instance1[ctr].plot_keywords, instance1[ctr].movie_imdb_link, instance1[ctr].num_user_for_reviews,
                            instance1[ctr].language,instance1[ctr].country, instance1[ctr].content_rating, instance1[ctr].budget,
                            instance1[ctr].title_year,instance1[ctr].actor_2_facebook_likes,instance1[ctr].imdb_score,instance1[ctr].aspect_ratio,
                            instance1[ctr].movie_facebook_likes);
                           
                            }
 

            }//if 
    }//while
    fclose(fp1);
    closedir(dir);  
    struct arg_struct *ar  = malloc(sizeof(struct arg_struct));
    strcpy(ar->f,outputstr);
    strcpy(ar->b,"allsort.csv");
    pthread_create(&ttid, NULL,readFileData,ar); 
    //printf("\nTid for thread %d  = %u",threadCount,&tid[threadCount]);
    pthread_join(ttid, NULL); 
     
 }



//method checks to see if the column entered by the user is a valid column
int isAColumn(char *str) //the str passed should be argv[2]
{

	int i, flag, columnToSort;
	
	flag = 0;
	columnToSort = sizeof(ctype)/sizeof(char *);

	for (i = 0; i < columnToSort; i++)
	{
		if (strcmp(ctype[i], str) == 0) 
        	{
         
            		flag = 1;
        	}
    	}
   return flag;	
}


//checks to see which column number is sorted
int columnNumberToSort(char *str2)
{

	int i, flag, nC,columnToSort;
    	nC = 2;
	flag = 0;
	columnToSort = sizeof(ctype)/sizeof(char *);

	for (i = 0; i < columnToSort; i++)
	{
		if (strcmp(ctype[i], str2) == 0) 
        	{
            		nC = i;
            		flag = 1;
        	}
    	} 

    return nC;

}

int main(int argc,char *argv[]){
	int i;
	int isColPresent = 0 ;
	printf("\n Main Thread id = %u\n", pthread_self());
    if(argc > 7 || argc < 3 ){
		printf("\nInvalid arguments...\n");
	}else{
		//-d -o optional to current dir
		//-c mandat.. for col
		for(i=0;i<argc;i++){
			if(strcmp(argv[i],"-d") == 0){
				strcpy(dirpath,argv[i+1]);//directory path 
			}else if(strcmp(argv[i],"-c") == 0 ){
				strcpy(col,argv[i+1]);//column that need to sort 
			  isColPresent = 1;
			}else if(strcmp(argv[i],"-o") == 0){
				strcpy(outputdir,argv[i+1]);//output directory 
			}
		}

		 	if(isColPresent == 1){
		 	   if(isAColumn(col)){ 
			 	 		numCol = columnNumberToSort(col);
			 	 	  listdir(dirpath,0);
			 	 }else{
			 		 printf("\nplease Provide correct column name!!! => %s\n",col);
					 exit(0); 	
			 	 }
			}else {
				printf("\nplease Provide column name!!!\n");
				exit(0);
			}
	} //else argc  
	mergeintoone();
    printf("\nTOTAL FILe = %d\n",count);
	printf("\nTOTAL CSV FILe = %d\n",csvCount);
    printf("\nTOTAL Threads = %d\n",threadCount);
  return(0);
}

void listdir(const char *name, int indent)
{

    DIR *dir;
    pthread_t tid[1024];
    struct dirent *entry;
    FILE *fp;
    char data[33];
    char path[1024];
    char readfile[1024];
    if (!(dir = opendir(name)))
        return;

    while ((entry = readdir(dir)) != NULL) {
        if (entry->d_type == DT_DIR) {
            
            if (strcmp(entry->d_name, ".") == 0 || strcmp(entry->d_name, "..") == 0)
                continue;
            snprintf(path, sizeof(path), "%s/%s", name, entry->d_name);
            //printf("%*s[%s]\n", indent, "", entry->d_name);
            //printf("\n|Printing path => %s|",path);
            listdir(path, indent + 2);
        } else {
            //printf("%*s- %s\n", indent, "", entry->d_name);
        	  //printf("\nFILE => |%s:%s| ",name,entry->d_name);
        	  char cddir[30] = {""};
        		strcpy(readfile,"");
        		
        		strcat(readfile,name);
        		strcat(readfile,"/");
        		strcat(readfile,entry->d_name);
        		//printf("\nFULL FILE = |%s|",readfile);
        		count++;
         	  //printf("\n==>%s\n", cddir);
        	  
        	  if(strstr(readfile,".csv")){
        	   	csvCount++;	
        	  	
        	  	//printf("\nSending Read File = %s",readfile);
        	  	//read - sort - write 
                struct arg_struct *ar  = malloc(sizeof(struct arg_struct));

                strcpy(ar->f,readfile);
                strcpy(ar->b,entry->d_name); 
        	  	pthread_create(&tid[threadCount], NULL,readFileData,ar);//creating thread and call function using thread 
      		    printf("\nTid for thread %d  = %u",threadCount,&tid[threadCount]);
                pthread_join(tid[threadCount++], NULL);// make sure to complete all thread before main close otherwise few files are not sorted...

        	  	//readFileData(readfile,entry->d_name);
        	  	 
        	  }
        }
    }
    closedir(dir);
}



void mergeSort(struct proj * arr , int l, int r, int colNum ){
    if (l == r)
    {
        return;
    }
    else 
    {
        int mid = ((l + r)/2);
        mergeSort(arr, l, mid,  colNum);
        mergeSort(arr, mid + 1, r, colNum);
        merge(arr, l, mid, mid + 1, r, colNum);
    }
}

void helper(struct proj *tArr, int fL, int fR, int sL, int sR, struct proj* instance1, int cnt, int first)
{
	while (fL <= fR)
            {

		
                tArr[cnt] = instance1[fL];
                fL++;
                cnt++;
            }


            while (sL <= sR)
            {

		////////////////MERGING STUFF
                tArr[cnt] = instance1[sL];
                sL++;
                cnt++;
            }
            
            
            for(fL = 0; fL < cnt; fL++)
            {
                //////////////////////////////
                instance1[first] = tArr[fL];
                first++;
            }


            free(tArr);  
}


/* MergeStart */
void merge (struct proj * instance1,int fL, int fR, int sL, int sR, int colNum )
{
     int cnt = 0;
    int first = fL;
    struct proj * tArr = (struct proj *) malloc(sizeof(struct proj)*((fR - fL + 1) + (sR - sL + 1)));
    switch(colNum)
 	{
		case 0 :	while(fL <= fR && sL <= sR){
                			if(strcmp((instance1[fL].color), (instance1[sL].color)) <= 0)
                				{
                	    			tArr[cnt] = instance1[fL];
                   				fL++;
            					}


                			else
                				{
                    				tArr[cnt] = instance1[sL];
                    				sL++;
                				}
					cnt++;
           			 }
				helper(tArr, fL, fR, sL, sR, instance1, cnt, first);
				break; 



		case 1 :	while(fL <= fR && sL <= sR){
                			if(strcmp((instance1[fL].director_name), (instance1[sL].director_name)) <= 0)
                				{
                	    			tArr[cnt] = instance1[fL];
                   				fL++;
            					}


                			else
                				{
                    				tArr[cnt] = instance1[sL];
                    				sL++;
                				}
					cnt++;
           			 }
				helper(tArr, fL, fR, sL, sR, instance1, cnt, first);
				break; 



		case 2 :	while(fL <= fR && sL <= sR){
                			if(instance1[fL].num_critic_for_reviews < instance1[sL].num_critic_for_reviews)
                				{
                	    			tArr[cnt] = instance1[fL];
                   				fL++;
            					}


                			else
                				{
                    				tArr[cnt] = instance1[sL];
                    				sL++;
                				}
					cnt++;
           			 }
				helper(tArr, fL, fR, sL, sR, instance1, cnt, first);
				break; 



		case 3 :	while(fL <= fR && sL <= sR){
                			if(instance1[fL].duration < instance1[sL].duration)
                				{
                	    			tArr[cnt] = instance1[fL];
                   				fL++;
            					}


                			else
                				{
                    				tArr[cnt] = instance1[sL];
                    				sL++;
                				}
					cnt++;
           			 }
				helper(tArr, fL, fR, sL, sR, instance1, cnt, first);
				break; 



		case 4 :	while(fL <= fR && sL <= sR){
                			if(instance1[fL].director_facebook_likes < instance1[sL].director_facebook_likes)
                				{
                	    			tArr[cnt] = instance1[fL];
                   				fL++;
            					}


                			else
                				{
                    				tArr[cnt] = instance1[sL];
                    				sL++;
                				}
					cnt++;
           			 }
				helper(tArr, fL, fR, sL, sR, instance1, cnt, first);
				break; 



		case 5 :	while(fL <= fR && sL <= sR){
                			if(instance1[fL].actor_3_facebook_likes < instance1[sL].actor_3_facebook_likes)
                				{
                	    			tArr[cnt] = instance1[fL];
                   				fL++;
            					}


                			else
                				{
                    				tArr[cnt] = instance1[sL];
                    				sL++;
                				}
					cnt++;
           			 }
				helper(tArr, fL, fR, sL, sR, instance1, cnt, first);
				break; 



		case 6 :	while(fL <= fR && sL <= sR){
                			if(strcmp((instance1[fL].actor_2_name), (instance1[sL].actor_2_name)) <= 0)
                				{
                	    			tArr[cnt] = instance1[fL];
                   				fL++;
            					}


                			else
                				{
                    				tArr[cnt] = instance1[sL];
                    				sL++;
                				}
					cnt++;
           			 }
				helper(tArr, fL, fR, sL, sR, instance1, cnt, first);
				break; 



		case 7 :	while(fL <= fR && sL <= sR){
                			if(instance1[fL].actor_1_facebook_likes < instance1[sL].actor_1_facebook_likes)
                				{
                	    			tArr[cnt] = instance1[fL];
                   				fL++;
            					}


                			else
                				{
                    				tArr[cnt] = instance1[sL];
                    				sL++;
                				}
					cnt++;
           			 }
				helper(tArr, fL, fR, sL, sR, instance1, cnt, first);
				break; 



		case 8 :	while(fL <= fR && sL <= sR){
                			if(instance1[fL].gross < instance1[sL].gross)
                				{
                	    			tArr[cnt] = instance1[fL];
                   				fL++;
            					}


                			else
                				{
                    				tArr[cnt] = instance1[sL];
                    				sL++;
                				}
					cnt++;
           			 }
				helper(tArr, fL, fR, sL, sR, instance1, cnt, first);
				break; 



		case 9 :	while(fL <= fR && sL <= sR){
                			if(strcmp((instance1[fL].genres), (instance1[sL].genres)) <= 0)
                				{
                	    			tArr[cnt] = instance1[fL];
                   				fL++;
            					}


                			else
                				{
                    				tArr[cnt] = instance1[sL];
                    				sL++;
                				}
					cnt++;
           			 }
				helper(tArr, fL, fR, sL, sR, instance1, cnt, first);
				break; 



		case 10 :	while(fL <= fR && sL <= sR){
                			if(strcmp((instance1[fL].actor_1_name), (instance1[sL].actor_1_name)) <= 0)
                				{
                	    			tArr[cnt] = instance1[fL];
                   				fL++;
            					}


                			else
                				{
                    				tArr[cnt] = instance1[sL];
                    				sL++;
                				}
					cnt++;
           			 }
				helper(tArr, fL, fR, sL, sR, instance1, cnt, first);
				break; 



		case 11 :	while(fL <= fR && sL <= sR){
                			if(strcmp((instance1[fL].movie_title), (instance1[sL].movie_title)) <= 0)
                				{
                	    			tArr[cnt] = instance1[fL];
                   				fL++;
            					}


                			else
                				{
                    				tArr[cnt] = instance1[sL];
                    				sL++;
                				}
					cnt++;
           			 }
				helper(tArr, fL, fR, sL, sR, instance1, cnt, first);
				break; 



		case 12 :	while(fL <= fR && sL <= sR){
                			if(instance1[fL].num_voted_users < instance1[sL].num_voted_users)
                				{
                	    			tArr[cnt] = instance1[fL];
                   				fL++;
            					}


                			else
                				{
                    				tArr[cnt] = instance1[sL];
                    				sL++;
                				}
					cnt++;
           			 }
				helper(tArr, fL, fR, sL, sR, instance1, cnt, first);
				break; 



		case 13 :	while(fL <= fR && sL <= sR){
                			if(instance1[fL].cast_total_facebook_likes < instance1[sL].cast_total_facebook_likes)
                				{
                	    			tArr[cnt] = instance1[fL];
                   				fL++;
            					}


                			else
                				{
                    				tArr[cnt] = instance1[sL];
                    				sL++;
                				}
					cnt++;
           			 }
				helper(tArr, fL, fR, sL, sR, instance1, cnt, first);
				break; 



		case 14 :	while(fL <= fR && sL <= sR){
                			if(strcmp((instance1[fL].actor_3_name), (instance1[sL].actor_3_name)) <= 0)
                				{
                	    			tArr[cnt] = instance1[fL];
                   				fL++;
            					}


                			else
                				{
                    				tArr[cnt] = instance1[sL];
                    				sL++;
                				}
					cnt++;
           			 }
				helper(tArr, fL, fR, sL, sR, instance1, cnt, first);
				break; 



		case 15 :	while(fL <= fR && sL <= sR){
                			if(instance1[fL].facenumber_in_poster < instance1[sL].facenumber_in_poster)
                				{
                	    			tArr[cnt] = instance1[fL];
                   				fL++;
            					}


                			else
                				{
                    				tArr[cnt] = instance1[sL];
                    				sL++;
                				}
					cnt++;
           			 }
				helper(tArr, fL, fR, sL, sR, instance1, cnt, first);
				break; 



		case 16 :	while(fL <= fR && sL <= sR){
                			if(strcmp((instance1[fL].plot_keywords), (instance1[sL].plot_keywords)) <= 0)
                				{
                	    			tArr[cnt] = instance1[fL];
                   				fL++;
            					}


                			else
                				{
                    				tArr[cnt] = instance1[sL];
                    				sL++;
                				}
					cnt++;
           			 }
				helper(tArr, fL, fR, sL, sR, instance1, cnt, first);
				break; 



		case 17 :	while(fL <= fR && sL <= sR){
                			if(strcmp((instance1[fL].movie_imdb_link), (instance1[sL].movie_imdb_link)) <= 0)
                				{
                	    			tArr[cnt] = instance1[fL];
                   				fL++;
            					}


                			else
                				{
                    				tArr[cnt] = instance1[sL];
                    				sL++;
                				}
					cnt++;
           			 }
				helper(tArr, fL, fR, sL, sR, instance1, cnt, first);
				break; 



		case 18 :	while(fL <= fR && sL <= sR){
                			if(instance1[fL].num_user_for_reviews < instance1[sL].num_user_for_reviews)
                				{
                	    			tArr[cnt] = instance1[fL];
                   				fL++;
            					}


                			else
                				{
                    				tArr[cnt] = instance1[sL];
                    				sL++;
                				}
					cnt++;
           			 }
				helper(tArr, fL, fR, sL, sR, instance1, cnt, first);
				break; 



		case 19 :	while(fL <= fR && sL <= sR){
                			if(strcmp((instance1[fL].language), (instance1[sL].language)) <= 0)
                				{
                	    			tArr[cnt] = instance1[fL];
                   				fL++;
            					}


                			else
                				{
                    				tArr[cnt] = instance1[sL];
                    				sL++;
                				}
					cnt++;
           			 }
				helper(tArr, fL, fR, sL, sR, instance1, cnt, first);
				break; 



		case 20 :	while(fL <= fR && sL <= sR){
                			if(strcmp((instance1[fL].country), (instance1[sL].country)) <= 0)
                				{
                	    			tArr[cnt] = instance1[fL];
                   				fL++;
            					}


                			else
                				{
                    				tArr[cnt] = instance1[sL];
                    				sL++;
                				}
					cnt++;
           			 }
				helper(tArr, fL, fR, sL, sR, instance1, cnt, first);
				break; 



		case 21 :	while(fL <= fR && sL <= sR){
                			if(strcmp((instance1[fL].content_rating), (instance1[sL].content_rating)) <= 0)
                				{
                	    			tArr[cnt] = instance1[fL];
                   				fL++;
            					}


                			else
                				{
                    				tArr[cnt] = instance1[sL];
                    				sL++;
                				}
					cnt++;
           			 }
				helper(tArr, fL, fR, sL, sR, instance1, cnt, first);
				break; 



		case 22 :	while(fL <= fR && sL <= sR){
                			if(instance1[fL].budget < instance1[sL].budget)
                				{
                	    			tArr[cnt] = instance1[fL];
                   				fL++;
            					}


                			else
                				{
                    				tArr[cnt] = instance1[sL];
                    				sL++;
                				}
					cnt++;
           			 }
				helper(tArr, fL, fR, sL, sR, instance1, cnt, first);
				break; 



		case 23 :	while(fL <= fR && sL <= sR){
                			if(instance1[fL].title_year < instance1[sL].title_year)
                				{
                	    			tArr[cnt] = instance1[fL];
                   				fL++;
            					}


                			else
                				{
                    				tArr[cnt] = instance1[sL];
                    				sL++;
                				}
					cnt++;
           			 }
				helper(tArr, fL, fR, sL, sR, instance1, cnt, first);
				break; 



		case 24 :	while(fL <= fR && sL <= sR){
                			if(instance1[fL].actor_2_facebook_likes < instance1[sL].actor_2_facebook_likes)
                				{
                	    			tArr[cnt] = instance1[fL];
                   				fL++;
            					}


                			else
                				{
                    				tArr[cnt] = instance1[sL];
                    				sL++;
                				}
					cnt++;
           			 }
				helper(tArr, fL, fR, sL, sR, instance1, cnt, first);
				break; 



		case 25 :	while(fL <= fR && sL <= sR){
                			if(instance1[fL].imdb_score < instance1[sL].imdb_score)
                				{
                	    			tArr[cnt] = instance1[fL];
                   				fL++;
            					}


                			else
                				{
                    				tArr[cnt] = instance1[sL];
                    				sL++;
                				}
					cnt++;
           			 }
				helper(tArr, fL, fR, sL, sR, instance1, cnt, first);
				break; 



		case 26 :	while(fL <= fR && sL <= sR){
                			if(instance1[fL].aspect_ratio < instance1[sL].aspect_ratio)
                				{
                	    			tArr[cnt] = instance1[fL];
                   				fL++;
            					}


                			else
                				{
                    				tArr[cnt] = instance1[sL];
                    				sL++;
                				}
					cnt++;
           			 }
				helper(tArr, fL, fR, sL, sR, instance1, cnt, first);
				break; 



		case 27 :	while(fL <= fR && sL <= sR){
                			if(instance1[fL].movie_facebook_likes < instance1[sL].movie_facebook_likes)
                				{
                	    			tArr[cnt] = instance1[fL];
                   				fL++;
            					}


                			else
                				{
                    				tArr[cnt] = instance1[sL];
                    				sL++;
                				}
					cnt++;
           			 }
				helper(tArr, fL, fR, sL, sR, instance1, cnt, first);
				break; 



		default : break;
	}
}
/*  MergEND*/   


void printEverything (struct proj* instance1,  int elementNo2,char filename[1024])
{
	int ctr;
 	 char tmpoutdir[1024];
 	 char tmpfilename[1024];
	 strcpy(tmpfilename,"correct_");
	 strcat(tmpfilename,filename);

	 strcpy(tmpoutdir,outputdir);
	  strcat(tmpoutdir,tmpfilename);
	  
	
	//printf("\n output dir = %s",tmpoutdir);
	FILE *fp = fopen(tmpoutdir,"w");
	 for(ctr = 1; ctr < elementNo2; ctr++)
    {
 //    	 printf("\n******|%s, %s, %d, %d, %d, %d, %s, %d, %d,%s,%s,%s,%d,%d,%s,%d,%s,%s,%d,%s,%s,%s,%d,%d,%d,%f,%f,%d", 
	// instance1[ctr].color,instance1[ctr].director_name,instance1[ctr].num_critic_for_reviews,instance1[ctr].duration,
	// instance1[ctr].director_facebook_likes,instance1[ctr].actor_3_facebook_likes,instance1[ctr].actor_2_name,
	// instance1[ctr].actor_1_facebook_likes, instance1[ctr].gross, instance1[ctr].genres,instance1[ctr].actor_1_name,
	// instance1[ctr].movie_title,instance1[ctr].num_voted_users, instance1[ctr].cast_total_facebook_likes, instance1[ctr].actor_3_name, 
	// instance1[ctr].facenumber_in_poster, instance1[ctr].plot_keywords, instance1[ctr].movie_imdb_link, instance1[ctr].num_user_for_reviews,
	// instance1[ctr].language,instance1[ctr].country, instance1[ctr].content_rating, instance1[ctr].budget,
	// instance1[ctr].title_year,instance1[ctr].actor_2_facebook_likes,instance1[ctr].imdb_score,instance1[ctr].aspect_ratio,
	// instance1[ctr].movie_facebook_likes);

     	fprintf(fp,"\n%s, %s, %d, %d, %d, %d, %s, %d, %d,%s,%s,%s,%d,%d,%s,%d,%s,%s,%d,%s,%s,%s,%d,%d,%d,%f,%f,%d", 
	instance1[ctr].color,instance1[ctr].director_name,instance1[ctr].num_critic_for_reviews,instance1[ctr].duration,
	instance1[ctr].director_facebook_likes,instance1[ctr].actor_3_facebook_likes,instance1[ctr].actor_2_name,
	instance1[ctr].actor_1_facebook_likes, instance1[ctr].gross, instance1[ctr].genres,instance1[ctr].actor_1_name,
	instance1[ctr].movie_title,instance1[ctr].num_voted_users, instance1[ctr].cast_total_facebook_likes, instance1[ctr].actor_3_name, 
	instance1[ctr].facenumber_in_poster, instance1[ctr].plot_keywords, instance1[ctr].movie_imdb_link, instance1[ctr].num_user_for_reviews,
	instance1[ctr].language,instance1[ctr].country, instance1[ctr].content_rating, instance1[ctr].budget,
	instance1[ctr].title_year,instance1[ctr].actor_2_facebook_likes,instance1[ctr].imdb_score,instance1[ctr].aspect_ratio,
	instance1[ctr].movie_facebook_likes);
   
    }
     fclose(fp);
}

//void *readFileData(char fname[1024],char basename[1024]){
void *readFileData(void *a){
		 //printf("\n====> File ===> %s",fname);
	struct arg_struct *ar  = a;

       char fname[1024];
       char basename[1024];
       strcpy(fname,ar->f);
        strcpy(basename,ar->b);
       
      	FILE *fp = fopen(fname,"r");
	 	struct proj * instance1 = (struct proj *)malloc(sizeof(struct proj)*35000); 
    
	
	 int columnCounter, elementNo,buffSize;	
	 char buffer[N];


	 columnCounter = 0;
	 elementNo = 0;
	 buffSize = sizeof(buffer);
	//fgets(buffer, buffSize, fp);
		while(fgets(buffer, buffSize, fp) != NULL)
		{
		
		
    		int i = 0; 
    		int columnToSort = 0;
    		
    		int counterForCommas = 1;
		while(buffer[counterForCommas] != '\0')
		{
			if(buffer[counterForCommas] == ',')
			{
				if(buffer[counterForCommas - 1] == ',')
				{
					char cma = buffer[counterForCommas];  //comma = cma	
					buffer[counterForCommas] = 'x';
					int newCounter = 1;

                			for(newCounter; buffer[counterForCommas + newCounter] < (1024- (counterForCommas + newCounter)); newCounter++)
                			{
                				char newChar = buffer[counterForCommas + newCounter];
                				buffer[counterForCommas + newCounter] = cma;
                				cma = newChar;
                				int counterForCommas2 = 0;
                			}	
            			}
        		}
        	counterForCommas++;
     	
    		}



		int xtemp,ytemp = 0;
		for(xtemp = 0; buffer[xtemp] != '\0'; xtemp++)
		{
			if(buffer[xtemp]=='"')
			{
				for(ytemp = (xtemp+1); buffer[ytemp] != '"'; ytemp++)
				{
					if(buffer[ytemp]==',')
					{
						buffer[ytemp]='^';
					}
				}
				xtemp = ytemp + 1;
			}
		}





    		char * tokenizedStr = strtok(buffer, ",");
    		int qCnt; //quoteCount
    
    		if(buffer[0] == ',')
    		{  
        		char * tok2 = "no input";
        		instance1[elementNo].color = (char *)malloc(sizeof(struct proj));
        		strcpy(instance1[elementNo].color, tok2);
        		columnToSort++;
    		}
    
    		while(tokenizedStr != NULL)
    		{
        		int lengthQuote = strlen(tokenizedStr);
        		for(qCnt = 0; qCnt < lengthQuote; qCnt++)
        		{
            			if(tokenizedStr[qCnt] == '"')
            			{
                			int p = 0;
                			for(p; p < lengthQuote; p++)//
               
                				if(tokenizedStr[p] == '^')
                				{
                    					tokenizedStr[p] = ',';
                				}
            			}
        	}	
            switch(columnToSort)
            {

		case 0 :  	instance1[elementNo].color = (char *)malloc(sizeof(struct proj));
                		strcpy(instance1[elementNo].color, tokenizedStr);
                		tokenizedStr = strtok(NULL, ",");
				break;

		case 1 :  	instance1[elementNo].director_name = (char *)malloc(sizeof(struct proj));
               			strcpy(instance1[elementNo].director_name, tokenizedStr);
                		tokenizedStr = strtok(NULL, ",");
				break;

		case 2 :  	instance1[elementNo].num_critic_for_reviews = atoi(tokenizedStr);
                		tokenizedStr = strtok(NULL, ",");
				break;

		case 3 :  	instance1[elementNo].duration = atoi(tokenizedStr);
                		tokenizedStr = strtok(NULL, ",");
				break;

		case 4 :  	instance1[elementNo].director_facebook_likes = atoi(tokenizedStr);
                		tokenizedStr = strtok(NULL, ",");
				break;

		case 5 :  	instance1[elementNo].actor_3_facebook_likes = atoi(tokenizedStr);
                		tokenizedStr = strtok(NULL, ",");
				break;

		case 6 :  	instance1[elementNo].actor_2_name = (char *)malloc(sizeof(struct proj));
                		strcpy(instance1[elementNo].actor_2_name, tokenizedStr);
                		tokenizedStr = strtok(NULL, ",");
				break;

		case 7 :  	instance1[elementNo].actor_1_facebook_likes = atoi(tokenizedStr);
                		tokenizedStr = strtok(NULL, ",");
				break;

		case 8 :  	instance1[elementNo].gross = atoi(tokenizedStr);
                		tokenizedStr = strtok(NULL, ",");
				break;

		case 9 :  	instance1[elementNo].genres = (char *)malloc(sizeof(struct proj));
                		strcpy(instance1[elementNo].genres, tokenizedStr);
                		tokenizedStr = strtok(NULL, ",");
				break;

		case 10 :  	instance1[elementNo].actor_1_name = (char *)malloc(sizeof(struct proj));
                		strcpy(instance1[elementNo].actor_1_name, tokenizedStr);
                		tokenizedStr = strtok(NULL, ",");
				break;

		case 11 :  	instance1[elementNo].movie_title = (char *)malloc(sizeof(struct proj));
                		strcpy(instance1[elementNo].movie_title, tokenizedStr);
                		tokenizedStr = strtok(NULL, ",");
				break;

		case 12 :  	instance1[elementNo].num_voted_users = atoi(tokenizedStr);
                		tokenizedStr = strtok(NULL, ",");
				break;

		case 13 :  	instance1[elementNo].cast_total_facebook_likes = atoi(tokenizedStr);
                		tokenizedStr = strtok(NULL, ",");
				break;

		case 14 :  	instance1[elementNo].actor_3_name = (char *)malloc(sizeof(struct proj));
                		strcpy(instance1[elementNo].actor_3_name, tokenizedStr);
                		tokenizedStr = strtok(NULL, ",");
				break;

		case 15 :  	instance1[elementNo].facenumber_in_poster = atoi(tokenizedStr);
                		tokenizedStr = strtok(NULL, ",");
				break;

		case 16 :  	instance1[elementNo].plot_keywords = (char *)malloc(sizeof(struct proj));
                		strcpy(instance1[elementNo].plot_keywords, tokenizedStr);
                		tokenizedStr = strtok(NULL, ",");
				break;

		case 17 :  	instance1[elementNo].movie_imdb_link = (char *)malloc(sizeof(struct proj));
                		strcpy(instance1[elementNo].movie_imdb_link, tokenizedStr);
                		tokenizedStr = strtok(NULL, ",");
				break;

		case 18 :  	instance1[elementNo].num_user_for_reviews = atoi(tokenizedStr);
                		tokenizedStr = strtok(NULL, ",");
				break;

		case 19 :  	instance1[elementNo].language = (char *) malloc(sizeof(struct proj));
                		strcpy(instance1[elementNo].language, tokenizedStr);
                		tokenizedStr = strtok(NULL, ",");
				break;

		case 20 :  	instance1[elementNo].country = (char *) malloc(sizeof(struct proj));
                		strcpy(instance1[elementNo].country, tokenizedStr);
                		tokenizedStr = strtok(NULL, ",");
				break;

		case 21:  	instance1[elementNo].content_rating = (char *) malloc(sizeof(struct proj));
                		strcpy(instance1[elementNo].content_rating, tokenizedStr);
                		tokenizedStr = strtok(NULL, ",");
				break;

		case 22 :  	instance1[elementNo].budget = atoi(tokenizedStr);
                		tokenizedStr = strtok(NULL, ",");
				break;

		case 23 :  	instance1[elementNo].title_year = atoi(tokenizedStr);
               			tokenizedStr = strtok(NULL, ",");
				break;

		case 24 :  	instance1[elementNo].actor_2_facebook_likes = atoi(tokenizedStr);
                		tokenizedStr = strtok(NULL, ",");
				break;

		case 25 :  	instance1[elementNo].imdb_score = atof(tokenizedStr);
                		tokenizedStr = strtok(NULL, ",");
				break;

		case 26 :  	instance1[elementNo].aspect_ratio = atof(tokenizedStr);
               		 	tokenizedStr = strtok(NULL, ",");
				break;

		case 27 :  	instance1[elementNo].movie_facebook_likes = atoi(tokenizedStr);
                		tokenizedStr = strtok(NULL, ",");
				break;

		default :       break;

		}

		

            columnToSort++;
            
            }
        elementNo++; //increments couter for instance1
    }
    int elementNo2 = elementNo;
    int parse;
    sizeOfStruct = elementNo2;
   
    int left = 0;
    
  
	mergeSort(instance1, left, sizeOfStruct - 1, numCol);
	
	printEverything(instance1,elementNo2,basename);
	end:
	 fclose(fp);
}









