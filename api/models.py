from .databases import Base, Base_config
from sqlalchemy import Column, Integer, String, Float, DateTime, \
    ForeignKey, UniqueConstraint,Boolean,Text
from sqlalchemy.ext.declarative import declared_attr


class BuildingInfo(Base):
    __tablename__ = 'building_info'

    code = Column(String(255), primary_key=True, unique=True, index=True)
    name = Column(String(255))
    area = Column(Float)
    population = Column(String(255))
    project_name = Column(String(255))
    project_code = Column(String(255))
    province = Column(String(255))
    city = Column(String(255))
    district = Column(String(255))
    project_function_type = Column(String(255))
    region_code = Column(String(10))
    domain_name = Column(String(20))


class BuldingEnergyCostBase(Base):
    """
    建筑能耗表，基表
    """
    __abstract__ = True

    id = Column(Integer, primary_key=True)
    total_energy = Column(Float)
    total_quota = Column(Float)
    total_guiding = Column(Float)
    time = Column(DateTime)

    @declared_attr
    def building_code(cls):
        return Column(String(255), ForeignKey("building_info.code"),nullable=False)
    __table_args__=(
    UniqueConstraint('building_code', 'time', name='idx_time_building_code'),)



class BuldingEnergyCostTotal(BuldingEnergyCostBase):
    """
    总能耗表，基表
    """
    __abstract__ = True

    total_clean = Column(Float)


class BuldingEnergyCostTotalH(BuldingEnergyCostTotal):
    __tablename__ = 'building_energy_cost_total_h'


class BuldingEnergyCostTotalD(BuldingEnergyCostTotal):
    __tablename__ = 'building_energy_cost_total_d'


class BuldingEnergyCostTotalM(BuldingEnergyCostTotal):
    __tablename__ = 'building_energy_cost_total_m'


class BuldingEnergyCostByType(BuldingEnergyCostBase):
    """
    能耗类型能耗表，基表
    """
    __abstract__ = True

    type = Column(String(255))
    __table_args__=(
    UniqueConstraint('building_code', 'time','type', name='idx_time_building_code_type'),)


class BuldingEnergyCostByTypeH(BuldingEnergyCostByType):
    __tablename__ = 'building_energy_cost_by_type_h'


class BuldingEnergyCostByTypeD(BuldingEnergyCostByType):
    __tablename__ = 'building_energy_cost_by_type_d'


class BuldingEnergyCostByTypeM(BuldingEnergyCostByType):
    __tablename__ = 'building_energy_cost_by_type_m'


class BuldingEnergyCostSub(BuldingEnergyCostBase):
    """
    电四大分项能耗表，基表
    """
    __abstract__ = True

    sub_name = Column(String(255))
    __table_args__=(
    UniqueConstraint('building_code', 'time','sub_name', name='idx_time_building_code_sub_name'),)


class BuldingEnergyCostSubH(BuldingEnergyCostSub):
    __tablename__ = 'building_energy_cost_sub_h'


class BuldingEnergyCostSubD(BuldingEnergyCostSub):
    __tablename__ = 'building_energy_cost_sub_d'


class BuldingEnergyCostSubM(BuldingEnergyCostSub):
    __tablename__ = 'building_energy_cost_sub_m'


class PlatformConfig(Base_config):
    __tablename__ = 'platform_config'
    platform_id = Column(Integer,primary_key=True)
    name = Column(String(255))
    inside_ip = Column(String(255))
    inside_port = Column(Integer)
    outside_ip = Column(String(255))
    outside_port = Column(Integer)
    domain_name = Column(String(255))
    created_time = Column(DateTime)
    destination = Column(String)
    receive_data_or_not = Column(Boolean)


class ProjectConfig(Base_config):
    __tablename__ = 'project_config'
    id = Column(Integer,primary_key=True)
    platform_id = Column(Integer)
    project_id = Column(Integer)
    project_name = Column(String(255))
    project_real_code = Column(String(255))
    database_name = Column(String(255))
    database_type = Column(String(255))
    database_user = Column(String(255))
    database_passwd = Column(String(255))
    database_ip = Column(String(255))
    database_port = Column(Integer)
    database_server_name = Column(String(255))
    real_ip = Column(String(255))
    real_port = Column(Integer)
    created_time = Column(DateTime)
    remark = Column(Text)
