import array

import pysam


def test_ecoli13(input_path, generate_bam=False, seqtender_test=False):
    if generate_bam:
        temp_bam = "/tmp/check.out"
        samfile = pysam.sort(input_path, "-o", temp_bam)
        samfile = pysam.index(temp_bam)
        samfile = pysam.AlignmentFile(temp_bam, "rb")

    else:
        samfile = pysam.AlignmentFile(input_path, "rb")

    it = samfile.fetch()
    sam_records = {(element.query_name, True if element.is_read1 else False): element for element in it}

    r790_first = sam_records[('r790', True)]

    assert r790_first.cigarstring == '35M'
    assert r790_first.get_forward_sequence() == 'CGTTCCCGTTTGTTTTATTTTTGTTAACATTTAAT'
    if not seqtender_test:
        assert r790_first.query_qualities == array.array('B',
                                                         [1, 1, 0, 0, 1, 1, 0, 0, 0, 1, 0, 0, 0, 1, 0, 2, 0, 1, 1, 4, 3,
                                                          3,
                                                          0, 5, 2, 0, 0, 0, 0, 0, 0, 1, 0, 1, 4])
    else:
        assert r790_first.query_qualities == array.array('B',
                                                         [31, 35, 34, 34, 34, 33, 32, 32, 32, 32, 31, 31, 31, 31, 30,
                                                          29, 28, 28, 28, 27, 26, 26, 24, 25, 24, 24, 24, 23, 22, 22,
                                                          22, 21, 20, 20, 19])

    r790_second = sam_records[('r790', False)]
    assert r790_second.cigarstring == '32M'
    assert r790_second.get_forward_sequence() == 'ATAATCAAAATTAACGAAAAAATGCCCTCTAT'
    if not seqtender_test:
        assert r790_second.query_qualities == array.array('B',
                                                          [2, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 1, 2, 2, 0, 1, 1, 1, 0,
                                                           0, 0,
                                                           2, 2, 2, 0, 3, 1, 2, 0, 4, 1])
    else:
        assert r790_second.query_qualities == array.array('B',
                                                          [21, 22, 22, 22, 23, 24, 24, 24, 25, 24, 26, 26, 27, 28, 28,
                                                           28, 29, 30, 31, 31, 31, 31, 32, 32, 32, 32, 33, 34, 34,
                                                           34, 35, 31])

    r780_first = sam_records[('r780', True)]
    assert r780_first.cigarstring == '35M'
    assert r780_first.get_forward_sequence() == 'AAGACCATCAAAATGAAATTTGGTCACCACGGCGG'
    if not seqtender_test:
        assert r780_first.query_qualities == array.array('B',
                                                         [1, 5, 1, 1, 1, 1, 0, 0, 0, 0, 0, 0, 0, 1, 4, 2, 2, 2, 1, 4, 3,
                                                          0,
                                                          0, 5, 0, 0, 0, 0, 0, 0, 0, 0, 1, 4, 3])
    else:
        assert r780_first.query_qualities == array.array('B',
                                                         [31, 35, 34, 34, 34, 33, 32, 32, 32, 32, 31, 31, 31, 31, 30,
                                                          29,
                                                          28, 28, 28, 27, 26, 26, 24, 25, 24, 24, 24, 23, 22, 22, 22,
                                                          21,
                                                          20, 20, 19])

    r780_second = sam_records[('r780', False)]
    assert r780_second.cigarstring == '32M'
    assert r780_second.get_forward_sequence() == 'TCGAACAGGGATTTATGTGTGACACGCAGGTT'
    if not seqtender_test:
        assert r780_second.query_qualities == array.array('B',
                                                          [1, 0, 0, 0, 0, 0, 0, 0, 2, 0, 0, 0, 1, 0, 0, 0, 0, 2, 1, 0,
                                                           0, 0, 0, 0, 1, 0, 1, 2, 1, 2, 1, 1])
    else:
        assert r780_second.query_qualities == array.array('B',
                                                          [21, 22, 22, 22, 23, 24, 24, 24, 25, 24, 26, 26, 27, 28, 28,
                                                           28,
                                                           29, 30, 31, 31, 31, 31, 32, 32, 32, 32, 33, 34, 34, 34, 35,
                                                           31])

    header = dict(samfile.header)
    assert header["HD"]['VN'] == '1.6'
    assert header["SQ"][0]['SN'] == 'e_coli:15000-66000'
    assert header["SQ"][0]['LN'] == 51001
    assert header['RG'][0]['ID'] == 'test_id'
    assert header['RG'][0]['SM'] == 'test_id'
    assert header['RG'][0]['LB'] == 'test_id'
    assert header['RG'][0]['PL'] == 'ILLUMINA'
    assert header['RG'][0]['PU'] == '0'


def get_info_field(record):
    merge = ""
    for item in record.info.items():
        merge += item[0]
        merge += "="
        if type(item[1]) == tuple:
            merge += str(item[1][0])
        else:
            merge += str(item[1])
        merge += ";"
    return merge


def test_freebayes(input_path):
    f = pysam.VariantFile(input_path, 'r')
    records = f.fetch()
    results = {record.pos: record for record in records}

    assert f.header.version == 'VCFv4.2'
    info_fields = {item[1].name: item[1] for item in f.header.info.items()}

    assert len(info_fields) == 51

    g_1000 = info_fields["1000G"]
    assert g_1000.number == 0
    assert g_1000.type == "Flag"
    assert g_1000.description == 'Membership in 1000 Genomes'

    ad = info_fields["AD"]
    assert ad.number == "R"
    assert ad.type == "Integer"
    assert ad.description == 'Total read depths for each allele'

    ann = info_fields["ANN"]
    assert ann.number == "."
    assert ann.type == "String"
    assert ann.description == "Functional annotations: 'Allele | Annotation | Annotation_Impact | Gene_Name | Gene_ID | Feature_Type | Feature_ID | Transcript_BioType | Rank | HGVS.c | HGVS.p | cDNA.pos / cDNA.length | CDS.pos / CDS.length | AA.pos / AA.length | Distance | ERRORS / WARNINGS / INFO'"

    cigar = info_fields["CIGAR"]
    assert cigar.number == "A"
    assert cigar.type == "String"
    assert cigar.description == 'Cigar string describing how to align alternate alleles to the reference allele'

    paired = info_fields["PAIRED"]
    assert paired.number == "A"
    assert paired.type == "Float"
    assert paired.description == 'Proportion of observed alternate alleles which are supported by properly paired read fragments'

    format_fields = {item[1].name: item[1] for item in f.header.formats.items()}

    assert len(format_fields) == 18

    dp = format_fields["DP"]
    assert dp.number == 1
    assert dp.type == "Integer"
    assert dp.description == 'Approximate read depth (reads with MQ=255 or with bad mates are filtered)'

    mq = format_fields["MQ"]
    assert mq.number == 1
    assert mq.type == "Float"
    assert mq.description == 'Root mean square (RMS) mapping quality'

    gt = format_fields["GT"]
    assert gt.number == 1
    assert gt.type == 'String'
    assert gt.description == 'Genotype'

    assert len(results) == 74

    pos_245 = results[245]
    assert pos_245.ref == 'ATTCA'
    assert pos_245.alts[0] == 'ATCA'
    assert pos_245.qual == 6.170000076293945
    assert ":".join([item[0] for item in pos_245.format.items()]) == 'GT:AD:AO:DP:PL:QA:QR:RO'
    assert pos_245.samples.items()[0][1].values() == [(1, 1), (0, 4), (4.0,), 4, (21, 12, 0), (23.0,), 0, 0]
    assert get_info_field(
        pos_245) == 'AB=0.0;ABP=0.0;AC=2;AF=1.0;AN=2;AO=4;CIGAR=1M1D3M;DP=4;DPB=3.200000047683716;DPRA=0.0;EPP=3.0102999210357666;EPPR=0.0;GTI=0;LEN=1;MEANALT=1.0;MQM=60.0;MQMR=0.0;NS=1;NUMALT=1;ODDS=1.1453200578689575;PAIRED=1.0;PAIREDR=0.0;PAO=0.0;PRO=0.0;RO=0;RPL=1.0;RPP=5.181769847869873;RPPR=0.0;RPR=3.0;RUN=1;SAF=1;SAP=5.181769847869873;SAR=3;SRF=0;SRP=0.0;SRR=0;TYPE=del;technology.ILLUMINA=1.0;'

    pos_14556 = results[14556]
    assert pos_14556.ref == 'AGGATG'
    assert pos_14556.alts[0] == 'AG'
    assert pos_14556.qual == 13.329999923706055
    assert ":".join([item[0] for item in pos_14556.format.items()]) == 'GT:AD:AO:DP:PL:QA:QR:RO'
    assert pos_14556.samples.items()[0][1].values() == [(1, 1), (0, 4), (4.0,), 4, (26, 12, 0), (28.0,), 0, 0]
    assert get_info_field(
        pos_14556) == 'AB=0.0;ABP=0.0;AC=2;AF=1.0;AN=2;AO=4;CIGAR=1M4D1M;DP=4;DPB=1.3333300352096558;DPRA=0.0;EPP=3.0102999210357666;EPPR=0.0;GTI=0;LEN=4;MEANALT=1.0;MQM=60.0;MQMR=0.0;NS=1;NUMALT=1;ODDS=3.0211598873138428;PAIRED=1.0;PAIREDR=0.0;PAO=0.0;PRO=0.0;RO=0;RPL=2.0;RPP=3.0102999210357666;RPPR=0.0;RPR=2.0;RUN=1;SAF=2;SAP=3.0102999210357666;SAR=2;SRF=0;SRP=0.0;SRR=0;TYPE=del;technology.ILLUMINA=1.0;'

    pos_41193 = results[41193]
    assert pos_41193.ref == 'GTTGTGA'
    assert pos_41193.alts[0] == 'GGA'
    assert pos_41193.qual == 3.049999952316284
    assert ":".join([item[0] for item in pos_41193.format.items()]) == 'GT:AD:AO:DP:PL:QA:QR:RO'
    assert pos_41193.samples.items()[0][1].values() == [(1, 1), (0, 7), (7.0,), 7, (19, 21, 0), (21.0,), 0, 0]
    assert get_info_field(
        pos_41193) == 'AB=0.0;ABP=0.0;AC=2;AF=1.0;AN=2;AO=7;CIGAR=1M4D2M;DP=7;DPB=3.0;DPRA=0.0;EPP=3.320509910583496;EPPR=0.0;GTI=0;LEN=4;MEANALT=1.0;MQM=60.0;MQMR=0.0;NS=1;NUMALT=1;ODDS=0.020083200186491013;PAIRED=1.0;PAIREDR=0.0;PAO=0.0;PRO=0.0;RO=0;RPL=2.0;RPP=5.802189826965332;RPPR=0.0;RPR=5.0;RUN=1;SAF=3;SAP=3.320509910583496;SAR=4;SRF=0;SRP=0.0;SRR=0;TYPE=del;technology.ILLUMINA=1.0;'

    pos_23112 = results[23112]
    assert pos_23112.ref == 'TAAAAAAGT'
    assert pos_23112.alts[0] == 'TAAAAGT'
    assert pos_23112.qual == 6.079999923706055
    assert ":".join([item[0] for item in pos_23112.format.items()]) == 'GT:AD:AO:DP:PL:QA:QR:RO'
    assert pos_23112.samples.items()[0][1].values() == [(1, 1), (0, 3), (3.0,), 3, (22, 9, 0), (24.0,), 0, 0]
    assert get_info_field(
        pos_23112) == 'AB=0.0;ABP=0.0;AC=2;AF=1.0;AN=2;AO=3;CIGAR=1M2D6M;DP=3;DPB=2.333329916000366;DPRA=0.0;EPP=3.7341198921203613;EPPR=0.0;GTI=0;LEN=2;MEANALT=1.0;MQM=60.0;MQMR=0.0;NS=1;NUMALT=1;ODDS=1.11667001247406;PAIRED=1.0;PAIREDR=0.0;PAO=0.0;PRO=0.0;RO=0;RPL=0.0;RPP=9.524720191955566;RPPR=0.0;RPR=3.0;RUN=1;SAF=1;SAP=3.7341198921203613;SAR=2;SRF=0;SRP=0.0;SRR=0;TYPE=del;technology.ILLUMINA=1.0;'

    pos_27234 = results[27234]
    assert pos_27234.ref == 'GCTC'
    assert pos_27234.alts[0] == 'GC'
    assert pos_27234.qual == 41.630001068115234
    assert ":".join([item[0] for item in pos_27234.format.items()]) == 'GT:AD:AO:DP:PL:QA:QR:RO'
    assert pos_27234.samples.items()[0][1].values() == [(1, 1), (0, 5), (5.0,), 5, (60, 15, 0), (65.0,), 0, 0]
    assert get_info_field(
        pos_27234) == 'AB=0.0;ABP=0.0;AC=2;AF=1.0;AN=2;AO=5;CIGAR=1M2D1M;DP=5;DPB=2.5;DPRA=0.0;EPP=3.4445900917053223;EPPR=0.0;GTI=0;LEN=2;MEANALT=1.0;MQM=60.0;MQMR=0.0;NS=1;NUMALT=1;ODDS=9.586130142211914;PAIRED=1.0;PAIREDR=0.0;PAO=0.0;PRO=0.0;RO=0;RPL=2.0;RPP=3.4445900917053223;RPPR=0.0;RPR=3.0;RUN=1;SAF=4;SAP=6.918950080871582;SAR=1;SRF=0;SRP=0.0;SRR=0;TYPE=del;technology.ILLUMINA=1.0;'


def test_vep(input_path):
    f = pysam.VariantFile(input_path, 'r')
    records = f.fetch()
    results = {record.pos: record for record in records}

    assert f.header.version == 'VCFv4.2'
    info_fields = {item[1].name: item[1] for item in f.header.info.items()}

    assert len(info_fields) == 24
    ac = info_fields["AC"]
    assert ac.number == 'A'
    assert ac.type == "Integer"
    assert ac.description == 'Allele count in genotypes, for each ALT allele, in the same order as listed'

    haplotype_score = info_fields["HaplotypeScore"]
    assert haplotype_score.number == 1
    assert haplotype_score.type == "Float"
    assert haplotype_score.description == 'Consistency of the site with at most two segregating haplotypes'

    format_fields = {item[1].name: item[1] for item in f.header.formats.items()}

    assert len(format_fields) == 7

    ad = format_fields["AD"]
    assert ad.number == 'R'
    assert ad.type == "Integer"
    assert ad.description == 'Allelic depths for the ref and alt alleles in the order listed'

    dp = format_fields["DP"]
    assert dp.number == 1
    assert dp.type == "Integer"
    assert dp.description == 'Approximate read depth (reads with MQ=255 or with bad mates are filtered)'

    filter_fields = {item[1].name: item[1] for item in f.header.filters.items()}

    assert len(filter_fields) == 16

    assert filter_fields["IndelReadPosRankSum"].description == 'ReadPosRankSum < -20.0'

    assert filter_fields["LowQual"].description == 'Low quality'

    assert filter_fields["IndelQD"].description == 'QD < 2.0'

    assert len(results) == 6

    pos_14397 = results[14397]
    assert pos_14397.ref == 'CTGT'
    assert pos_14397.alts[0] == 'C'
    assert pos_14397.qual == 139.1199951171875
    assert ":".join([item[0] for item in pos_14397.format.items()]) == 'GT:AD:DP:FT:GQ:PL'
    assert pos_14397.samples.items()[0][1].values() == [(0, 1), (16, 4), 20, ('rd',), 99, (120, 0, 827)]
    assert pos_14397.samples.items()[1][1].values() == [(0, 1), (8, 2), 10, ('dp;rd',), 60, (60, 0, 414)]
    assert pos_14397.samples.items()[2][1].values() == [(0, 0), (39, 0), 39, ('PASS',), 99, (0, 116, 2114)]
    assert get_info_field(
        pos_14397) == 'AC=2;AF=0.3330000042915344;AN=6;BaseQRankSum=1.7999999523162842;CSQ=-|downstream_gene_variant|MODIFIER|DDX11L1|ENSG00000223972|Transcript|ENST00000450305|transcribed_unprocessed_pseudogene|||||||||||728|1||HGNC|HGNC:37102;ClippingRankSum=0.1379999965429306;DP=69;FS=7.785999774932861;MLEAC=2;MLEAF=0.3330000042915344;MQ=26.84000015258789;MQ0=0;MQRankSum=-1.906000018119812;QD=1.5499999523162842;ReadPosRankSum=0.3840000033378601;'

    pos_19190 = results[19190]
    assert pos_19190.ref == 'GC'
    assert pos_19190.alts[0] == 'G'
    assert pos_19190.qual == 1186.8800048828125
    assert ":".join([item[0] for item in pos_19190.format.items()]) == 'GT:AD:DP:FT:GQ:PL'
    assert pos_19190.samples.items()[0][1].values() == [(0, 1), (8, 14), 22, ('PASS',), 99, (416, 0, 201)]
    assert pos_19190.samples.items()[1][1].values() == [(0, 1), (18, 13), 31, ('PASS',), 99, (353, 0, 503)]
    assert pos_19190.samples.items()[2][1].values() == [(0, 1), (5, 15), 20, ('rd',), 99, (457, 0, 107)]
    assert get_info_field(
        pos_19190) == 'AC=3;AF=0.5;AN=6;BaseQRankSum=4.1570000648498535;CSQ=-|downstream_gene_variant|MODIFIER|DDX11L1|ENSG00000223972|Transcript|ENST00000456328|lncRNA|||||||||||4782|1||HGNC|HGNC:37102;ClippingRankSum=3.6659998893737793;DP=74;FS=37.0369987487793;MLEAC=3;MLEAF=0.5;MQ=22.260000228881836;MQ0=0;MQRankSum=0.19499999284744263;QD=16.040000915527344;ReadPosRankSum=-4.072000026702881;'


def test_vcftools(input_path):
    f = pysam.VariantFile(input_path, 'r')
    records = f.fetch()
    results = {record.pos: record for record in records}

    assert f.header.version == 'VCFv4.2'
    info_fields = {item[1].name: item[1] for item in f.header.info.items()}
    assert len(info_fields) == 23
    ac = info_fields["AC"]
    assert ac.number == 'A'
    assert ac.type == "Integer"
    assert ac.description == 'Allele count in genotypes, for each ALT allele, in the same order as listed'

    haplotype_score = info_fields["HaplotypeScore"]
    assert haplotype_score.number == 1
    assert haplotype_score.type == "Float"
    assert haplotype_score.description == 'Consistency of the site with at most two segregating haplotypes'

    format_fields = {item[1].name: item[1] for item in f.header.formats.items()}

    assert len(format_fields) == 7

    ad = format_fields["AD"]
    assert ad.number == 'R'
    assert ad.type == "Integer"
    assert ad.description == 'Allelic depths for the ref and alt alleles in the order listed'

    dp = format_fields["DP"]
    assert dp.number == 1
    assert dp.type == "Integer"
    assert dp.description == 'Approximate read depth (reads with MQ=255 or with bad mates are filtered)'

    filter_fields = {item[1].name: item[1] for item in f.header.filters.items()}

    assert len(filter_fields) == 16

    assert filter_fields["IndelReadPosRankSum"].description == 'ReadPosRankSum < -20.0'

    assert filter_fields["LowQual"].description == 'Low quality'

    assert filter_fields["IndelQD"].description == 'QD < 2.0'

    pos_63735 = results[63735]
    assert pos_63735.ref == 'CCTA'
    assert pos_63735.alts[0] == 'C'
    assert pos_63735.qual == 2994.090087890625
    assert ":".join([item[0] for item in pos_63735.format.items()]) == 'GT:AD:DP:FT:GQ:PL'
    assert pos_63735.samples.items()[0][1].values() == [(0, 0), (27, 0), 27, ('PASS',), 79, (0, 79, 1425)]
    assert pos_63735.samples.items()[1][1].values() == [(0, 0), (40, 0), 40, ('PASS',), 99, (0, 117, 2120)]
    assert pos_63735.samples.items()[2][1].values() == [(0, 1), (23, 74), 97, ('rd',), 99, (3034, 0, 942)]
    assert get_info_field(
        pos_63735) == 'AC=1;AF=0.16699999570846558;AN=6;BaseQRankSum=1.1380000114440918;ClippingRankSum=0.4480000138282776;DB=True;DP=176;FS=13.597000122070312;MLEAC=1;MLEAF=0.16699999570846558;MQ=31.059999465942383;MQ0=0;MQRankSum=0.6359999775886536;QD=9.979999542236328;ReadPosRankSum=-1.1799999475479126;'

    pos_752721 = results[752721]
    assert pos_752721.ref == 'A'
    assert pos_752721.alts[0] == 'G'
    assert pos_752721.qual == 2486.89990234375
    assert ":".join([item[0] for item in pos_752721.format.items()]) == 'GT:AD:DP:FT:GQ:PL'
    assert pos_752721.samples.items()[0][1].values() == [(1, 1), (0, 27), 27, ('PASS',), 81, (1021, 81, 0)]
    assert pos_752721.samples.items()[1][1].values() == [(1, 1), (0, 19), 19, ('dp',), 57, (661, 57, 0)]
    assert pos_752721.samples.items()[2][1].values() == [(1, 1), (0, 22), 22, ('PASS',), 66, (831, 66, 0)]
    assert get_info_field(
        pos_752721) == 'AC=6;AF=1.0;AN=6;DB=True;DP=69;FS=0.0;MLEAC=6;MLEAF=1.0;MQ=60.0;MQ0=0;POSITIVE_TRAIN_SITE=True;QD=31.670000076293945;VQSLOD=18.940000534057617;culprit=QD;'

    pos_752791 = results[752791]
    assert pos_752791.ref == 'A'
    assert pos_752791.alts[0] == 'G'
    assert pos_752791.qual == 2486.89990234375
    assert ":".join([item[0] for item in pos_752791.format.items()]) == 'GT:AD:DP:FT:GQ:PL:SB'
    assert pos_752791.samples.items()[0][1].values() == [(1, 1), (0, 27), 27, ('PASS',), 81, (1021, 81, 0),
                                                         (0, 1, 2, 3)]
    assert pos_752791.samples.items()[1][1].values() == [(1, 1), (0, 19), 19, ('dp',), 57, (661, 57, 0), (4, 5, 6, 7)]
    assert pos_752791.samples.items()[2][1].values() == [(1, 1), (0, 22), 22, ('PASS',), 66, (831, 66, 0), (2, 3, 4, 5)]
    assert get_info_field(
        pos_752791) == 'AC=6;AF=1.0;AN=6;DB=True;DP=69;FS=0.0;MLEAC=6;MLEAF=1.0;MQ=60.0;MQ0=0;POSITIVE_TRAIN_SITE=True;QD=31.670000076293945;VQSLOD=18.940000534057617;culprit=QD;'
