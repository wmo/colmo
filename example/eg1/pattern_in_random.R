

options(echo=FALSE) # if you want see commands in output file
args <- commandArgs(trailingOnly = TRUE)
#print(args)

pattern<-"0000"

if (length(args)>0) { 
    pattern<-args[1]
}
cnt<-0

go_on<-TRUE
while (go_on) {
    cnt<-cnt+1
    v<-toString(floor(abs(rnorm(1,0,10000000))))
    if (nchar(v)>nchar(pattern)) {
        vs<-substr(v,nchar(v)-nchar(pattern)+1,nchar(v))
        if (pattern==vs) {
            print( sprintf("| R | pattern-length:%d | iterations:%d | found:%s |", nchar(pattern), cnt,v ) )
            go_on<-FALSE
        }
    }
}
