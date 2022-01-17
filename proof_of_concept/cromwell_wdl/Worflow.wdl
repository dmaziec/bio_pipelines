version 1.0 

#STRUCTS 
struct ReferenceFasta {
  File ref_dict
  File ref_fasta
  File ref_fasta_index
  File ref_sa
  File ref_amb
  File ref_bwt
  File ref_ann
  File ref_pac
}

#TASKS

task alignment {
  input {
    String name
    File read
    ReferenceFasta reference
    String read_group
  }
  command {
    bwa mem -R ~{read_group} ~{reference.ref_fasta}  ~{read} | samtools view -bS - > ${name}.bam
    mkdir mapped 
    mv ${name}.bam mapped/
  }
  runtime {
    docker: "quay.io/biocontainers/mulled-v2-ad317f19f5881324e963f6a6d464d696a2825ab6:c59b7a73c87a9fe81737d5d628e10a3b5807f453-0"
  }
  output {
    File mapped = "mapped/${name}.bam"
  }
}


task sorting{
  input {
    File mapped 
    String name
  }
  command {
    mkdir sorted 
    gatk SortSam --INPUT ${mapped} --OUTPUT sorted/${name}.sam --SORT_ORDER coordinate
  }
  runtime {
    docker: "broadinstitute/gatk:latest"
  }
  output { 
    File sorted = "sorted/${name}.sam"
  }
}

task mark_duplicates {
  input {
    File sorted 
    String name
  }
  command {
    gatk MarkDuplicates --INPUT ${sorted} --OUTPUT ${name}_marked_duplicates.bam -M ${name}_marked_duplicates.txt
  }
  runtime {
    docker: "broadinstitute/gatk:latest"
  }
  output {
    File marked_duplicates_bam = "${name}_marked_duplicates.bam"
    File marked_duplicates_txt = "${name}_marked_duplicates.txt"
  }
}
task index_marked {
  input {
    String name
    File marked
  }
  command {
    mkdir index
    cp ${marked} index/
    samtools index index/${name}_marked_duplicates.bam
  }
  runtime {
    docker: "biocontainers/samtools:v1.9-4-deb_cv1"
  }
  output {
    File index_mark = "index/${name}_marked_duplicates.bam"
    File index_mark_bai = "index/${name}_marked_duplicates.bam.bai"

  }
}

task haplotype_caller{
  input {
    File mapped
    File mapped_bai
    File mapped_text
    ReferenceFasta reference
    String name
  }
  command {
    mkdir haplotype_caller
    cp ${mapped}  haplotype_caller/
    cp ${mapped_bai}  haplotype_caller/
    cp ${mapped_text} haplotype_caller/
    gatk HaplotypeCaller --reference ${reference.ref_fasta} --input haplotype_caller/${name}_marked_duplicates.bam --output haplotype_caller/${name}_HaplotypeCaller.g.vcf
  }
  runtime {
    docker: "quay.io/biocontainers/gatk4:4.1.8.0--py38h37ae868_0"
  }
  output {
    File haplotype_caller = "haplotype_caller/${name}_HaplotypeCaller.g.vcf"
  }
}
#task vep{}

#WORKFLOW 

workflow NGS {
  input {
    ReferenceFasta reference
    Array[Object] reads 
  }

  scatter(read in reads) {
    call alignment {
      input:
        name = read.name,
        read = read.directory,
        reference = reference,
        read_group = read.group_read,
    }
    call sorting {
      input: 
        name = read.name,
        mapped = alignment.mapped
    }
    call mark_duplicates {
     input:
      sorted = sorting.sorted,
      name = read.name
    }

    call index_marked {
      input: 
        marked = mark_duplicates.marked_duplicates_bam,
        name = read.name
    }
    call haplotype_caller {
      input: 
        mapped =  mark_duplicates.marked_duplicates_bam,
        reference = reference,
        name = read.name,
        mapped_bai = index_marked.index_mark_bai,
        mapped_text = mark_duplicates.marked_duplicates_txt
    }
  }
  }

