% by default look for 4 zeroes
pattern="0000"; % to match at the end

% arguments received from the command line
arg_list = argv();
if nargin>0
    pattern=arg_list{1};
end

cnt=0;
go_on=1;
while (go_on==1)  
    v=num2str(floor(abs(normrnd(0,10000000))));
    cnt=cnt+1;
    if (length(v)>length(pattern)) && (rindex(v,pattern)==(length(v)-length(pattern)+1))
        go_on=0;
        printf('| Octave | pattern-length:%d | iterations:%d | found:%s |', length(pattern), cnt,v ) 
    end
end

