import os.path

""" USED IN THE DYNAMIC BASED PIPELINE"""


def interleave_fastq(paired_end1, paired_end2, samples, **kwargs):
    output = samples.generate_interleaved_fastq_file_name(paired_end1, paired_end2)
    if os.path.isfile(output):
        print("Fail already exists")

    def read_records(file, num):
        lines = True
        while lines:
            record = []
            for line in range(4):
                try:
                    l = next(file)
                    if line == 0:
                        ll = l.replace("\n", num + "\n")
                        record += [ll]
                    else:
                        record += [l]
                except StopIteration:
                    if len(record) == 0:
                        lines = False
                        yield None
                    else:
                        raise Exception("The incomplete record in ", file, " found.")

            yield ''.join(record)

    with open(paired_end1, 'r') as p1:
        with open(paired_end2, 'r') as p2:
            with open(output, 'a') as output:
                p1_records = read_records(p1, "/1")
                p2_records = read_records(p2, "/2")

                for record1, record2 in zip(p1_records, p2_records):
                    if record1 is not None and record2 is not None:
                        output.write(record1)
                        # output.write('\n')
                        output.write(record2)


"""def generate_interleaved_fastq_file_name(fastq1, fastq2):
    for r in (("_1", ""), ("_2", "")):
        fastq1 = fastq1.replace(*r)
        fastq2 = fastq2.replace(*r)
    if fastq1 == fastq2:
        return fastq2
    else:
        return None


r1 = "/Users/dominika/projects/bowtie2/example/reads/adamformat/read_1.fq"
r2 = "/Users/dominika/projects/bowtie2/example/reads/adamformat/read_2.fq"
interleaved = generate_interleaved_fastq_file_name(r1, r2)
interleave_fastq(r1,
                 r2,
                 interleaved)
"""
