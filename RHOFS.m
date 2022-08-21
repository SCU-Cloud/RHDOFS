function [ selectedFeatures,time ] = RHOFS( X, Y)
% diff: dep_Mean=dep_Set/sum(mode);


start=tic;
[card_U,p]=size(X);

mode=zeros(1,p);                                             
dep_Mean=0;                                                    
dep_Set=0;                                                 
depArray=zeros(1,p);    
C=unique(Y);
card_C=length(C);
centers=zeros(card_C,p);
rhmats=zeros(card_C,card_U,p);
% ct=1;
for i=1:p
     col=X(:,i);
     [rhmatcol, center]= rhmat_col(col,Y);
     centers(:,i)=center;
     rhmats(:,:,i)=rhmatcol;
     
     dep_single=dep(rhmatcol);
     depArray(1,i)=dep_single;
    if dep_single>dep_Mean 
%         ss(ct)=i;
%         dd(ct)=dep_Mean;
%         ct=ct+1;
            mode(1,i)=1;  
            rhmats_temp=rhmats(:,:,mode==1);
            mat=ones(card_C,card_U,1);
            for j=1:sum(mode)
                mat = mat&rhmats_temp(:,:,j);
            end
            dep_New=dep(mat);
            if dep_New>dep_Set                             
                   dep_Set=dep_New;
                   dep_Mean=dep_Set/sum(mode);
            elseif dep_New==dep_Set                                                        
                   [index_del] = non_signf(rhmats,mode,i,card_C,card_U,3);
                   if sum(index_del)==0
                       mode(1,i)=0;
                   end
                   if sum(index_del)>1
                       mode(1,index_del)=0;
                   end
                   if sum(index_del)==1
                       ind_del=find(index_del==1);
                       if dep_New<1
                           info_i = info_BN(rhmats,mode,ind_del,X,Y,centers,card_C,card_U);
                           info_del = info_BN(rhmats,mode,i,X,Y,centers,card_C,card_U);
                           if info_del>info_i
                               mode(1,i)=0;
                           elseif info_del<info_i
                               mode(1,index_del)=0;
                           else
                               if rand>0.5
                                   mode(1,i)=0;
                               else
                                   mode(1,index_del)=0;
                               end
                           end
                       end
                       if dep_New==1
                           model_tem=zeros(1,p);
                           model_tem(1,i)=1;
                           model_tem(1,index_del)=1;
                           info_i = info_BN(rhmats,model_tem,ind_del,X,Y,centers,card_C,card_U);
                           info_del = info_BN(rhmats,model_tem,i,X,Y,centers,card_C,card_U);
                           if depArray(ind_del)+info_del>depArray(i)+info_i
                               mode(1,i)=0;
                           elseif depArray(ind_del)+info_del<depArray(i)+info_i
                               mode(1,index_del)=0;
                           else
                               if rand>0.5
                                   mode(1,i)=0;
                               else
                                   mode(1,index_del)=0;
                               end
                           end
                       end
                   end
                   dep_Mean=dep_Set/sum(mode);
           else
                    mode(1,i)=0;   
           end
    end
    
end

selectedFeatures=find(mode==1);

time=toc(start);    
end

function [ rhmatcol, center ] = rhmat_col(data,Y)

[n,~]=size(data);
C=unique(Y);
card_C=length(C);
class = cell(1,card_C);
for i = 1:card_C
    class{i} = [];
end
for i=1:n
    class{find(C==Y(i,1))} = [class{find(C==Y(i,1))};data(i,1)];
end

Lmat = zeros(card_C,1);
Umat = zeros(card_C,1);
for i = 1:card_C
    Lmat(i,1)=min(class{i}(:,1));
    Umat(i,1)=max(class{i}(:,1));
end

rhmat = zeros(card_C,n);
for i = 1:n
    for j = 1: card_C
        if Lmat(j,1) <= data(i,1) &&  data(i,1) <= Umat(j,1)
            rhmat(j,i) = 1;
        end
    end
end

rhmatcol = rhmat;
center_col = zeros(card_C,1);
for i = 1:card_C
    center_col(i,1) = mean(class{i});
end
center = center_col;
end

function [ dep ] = dep(rhmat)

[~,card_U]=size(rhmat);
card_Pos=0;
for i = 1:card_U
    card_Pos=card_Pos+1-min(sum(rhmat(:,i))-1,1);
end
dep=card_Pos/card_U;
end


function [index_del] = non_signf(rhmats,mode,i,card_C,card_U,k)
BB=zeros(1,length(mode));
for iter=1:k
    B=zeros(1,length(mode));
    R=mode;
    T=mode;
    T(1,i)=0;

    while sum(T)>0
        indexs=find(T==1);
        Num=length(indexs);
        A=randperm(Num);
        rnd=A(1);
        ind=indexs(rnd);
        if sig(rhmats,R,ind,card_C,card_U)==0
            B(1,ind)=1;
            R(1,ind)=0;
        end
        T(1,ind)=0;
    end
    if sum(B)>sum(BB)
        BB = B;
    end
end
index_del=BB==1;
end

function [s]= sig(rhmats,mode,f_i,card_C,card_U)
mode(1,f_i)=0;
rhmats_temp=rhmats(:,:,mode==1);
mat=ones(card_C,card_U,1);
for i=1:sum(mode)
    mat = mat&rhmats_temp(:,:,i);
end
reshape(mat,card_C,card_U);
[d_F]=dep(mat);
mat = mat&rhmats(:,:,f_i);
[d_B]=dep(mat);
s=d_B-d_F;
end

function [info]= info_BN(rhmats,mode,f_i,data,Y,centers,card_C,card_U)
mode(1,f_i)=0;
rhmats_temp=rhmats(:,:,mode==1);
mat=ones(card_C,card_U,1);
for i=1:sum(mode)
    mat = mat&rhmats_temp(:,:,i);
end
info=0;
C=unique(Y);
for i=1:card_U
    if sum(mat(:,i))>1
        center_i=find(C==Y(i,1));
        mem=membership(data(i,:),centers,mode,center_i);
        info=info+mem;
    end
end
info=info/card_U;
end

function [dist]= euclid_dist2(a,b)
dist=sum((a-b).^2);
end
function [mem]= membership(point,centers,mode,center_i)
center=centers(:,mode==1);
point=point(:,mode==1);
[card_C,~]=size(center);
dists=zeros(1,card_C);
for i=1:card_C
    dists(i) = euclid_dist2(point,center(i,:));
end
mem=1/(sum(1./dists)*dists(center_i));
end



