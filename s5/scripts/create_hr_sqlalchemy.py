"""Create HR database using SQLAlchemy ORM.

Same code, different DATABASE_URL. This is the power of an ORM.

Usage:
    python create_hr_sqlalchemy.py                # SQLite (default)
    python create_hr_sqlalchemy.py postgresql      # PostgreSQL via docker-compose
"""

import sys
from datetime import date
from pathlib import Path


def d(s: str) -> date:
    return date.fromisoformat(s)


from sqlalchemy import (
    Column,
    Date,
    DateTime,
    Float,
    ForeignKey,
    Integer,
    String,
    Text,
    create_engine,
    func,
)
from sqlalchemy.orm import Session, declarative_base, relationship

SETTINGS = {
    "sqlite": f"sqlite:///{Path(__file__).parent.parent / 'hr_database.db'}",
    "url": "postgresql://postgres:ericevaericeva9@bdi-hr-database.cfowxstntumk.us-east-1.rds.amazonaws.com:5432/hr_database",
}

Base = declarative_base()


class Department(Base):
    __tablename__ = "department"
    id = Column(Integer, primary_key=True)
    name = Column(String(100), nullable=False, unique=True)
    location = Column(String(100))
    created_at = Column(DateTime, server_default=func.now())
    employees = relationship("Employee", back_populates="department")
    projects = relationship("Project", back_populates="department")


class Employee(Base):
    __tablename__ = "employee"
    id = Column(Integer, primary_key=True)
    first_name = Column(String(50), nullable=False)
    last_name = Column(String(50), nullable=False)
    email = Column(String(100), nullable=False, unique=True)
    hire_date = Column(Date, nullable=False)
    salary = Column(Float, nullable=False)
    department_id = Column(Integer, ForeignKey("department.id"))
    created_at = Column(DateTime, server_default=func.now())
    department = relationship("Department", back_populates="employees")
    projects = relationship("EmployeeProject", back_populates="employee")
    salary_history = relationship("SalaryHistory", back_populates="employee")


class Project(Base):
    __tablename__ = "project"
    id = Column(Integer, primary_key=True)
    name = Column(String(100), nullable=False)
    description = Column(Text)
    start_date = Column(Date, nullable=False)
    end_date = Column(Date)
    budget = Column(Float)
    department_id = Column(Integer, ForeignKey("department.id"))
    department = relationship("Department", back_populates="projects")
    employees = relationship("EmployeeProject", back_populates="project")


class EmployeeProject(Base):
    __tablename__ = "employee_project"
    employee_id = Column(Integer, ForeignKey("employee.id"), primary_key=True)
    project_id = Column(Integer, ForeignKey("project.id"), primary_key=True)
    role = Column(String(50), default="member")
    assigned_date = Column(Date)
    employee = relationship("Employee", back_populates="projects")
    project = relationship("Project", back_populates="employees")


class SalaryHistory(Base):
    __tablename__ = "salary_history"
    id = Column(Integer, primary_key=True)
    employee_id = Column(Integer, ForeignKey("employee.id"), nullable=False)
    old_salary = Column(Float, nullable=False)
    new_salary = Column(Float, nullable=False)
    change_date = Column(Date, nullable=False)
    reason = Column(String(200))
    employee = relationship("Employee", back_populates="salary_history")


def seed_data(session: Session) -> None:
    eng = Department(name="Engineering", location="Barcelona")
    mkt = Department(name="Marketing", location="Madrid")
    hr = Department(name="Human Resources", location="Barcelona")
    fin = Department(name="Finance", location="London")
    sales = Department(name="Sales", location="New York")
    session.add_all([eng, mkt, hr, fin, sales])
    session.flush()

    employees_data = [
        ("Anna", "Garcia", "anna.garcia@company.com", d("2020-03-15"), 55000.00, eng),
        ("Marc", "Lopez", "marc.lopez@company.com", d("2019-07-01"), 62000.00, eng),
        ("Laura", "Martinez", "laura.martinez@company.com", d("2021-01-10"), 48000.00, mkt),
        ("Carlos", "Fernandez", "carlos.fernandez@company.com", d("2018-11-20"), 70000.00, eng),
        ("Sofia", "Rodriguez", "sofia.rodriguez@company.com", d("2022-06-01"), 45000.00, hr),
        ("David", "Sanchez", "david.sanchez@company.com", d("2020-09-15"), 58000.00, fin),
        ("Maria", "Diaz", "maria.diaz@company.com", d("2017-04-01"), 75000.00, fin),
        ("Pablo", "Ruiz", "pablo.ruiz@company.com", d("2023-02-01"), 42000.00, sales),
        ("Elena", "Torres", "elena.torres@company.com", d("2021-08-15"), 52000.00, mkt),
        ("Jorge", "Navarro", "jorge.navarro@company.com", d("2019-12-01"), 60000.00, eng),
        ("Clara", "Moreno", "clara.moreno@company.com", d("2022-03-15"), 47000.00, sales),
        ("Ivan", "Jimenez", "ivan.jimenez@company.com", d("2020-05-20"), 53000.00, eng),
    ]
    emps = []
    for first, last, email, hire, salary, dept in employees_data:
        emp = Employee(
            first_name=first,
            last_name=last,
            email=email,
            hire_date=hire,
            salary=salary,
            department=dept,
        )
        session.add(emp)
        emps.append(emp)
    session.flush()

    anna, marc, laura, carlos, sofia, david, maria, pablo, elena, jorge, clara, ivan = emps

    projects_data = [
        ("Data Platform", "Build the internal data platform", d("2024-01-15"), d("2024-12-31"), 150000.00, eng),
        ("Brand Refresh", "Company rebranding campaign", d("2024-03-01"), d("2024-09-30"), 80000.00, mkt),
        ("HR Portal", "Employee self-service portal", d("2024-02-01"), None, 60000.00, hr),
        ("Q4 Budget", "Annual budget planning", d("2024-07-01"), d("2024-10-31"), 25000.00, fin),
        ("Mobile App", "Customer mobile application", d("2024-04-15"), None, 200000.00, eng),
        ("Sales Dashboard", "Real-time sales analytics", d("2024-05-01"), d("2024-11-30"), 45000.00, sales),
    ]
    projs = []
    for name, desc, start, end, budget, dept in projects_data:
        proj = Project(
            name=name,
            description=desc,
            start_date=start,
            end_date=end,
            budget=budget,
            department=dept,
        )
        session.add(proj)
        projs.append(proj)
    session.flush()

    data_platform, brand_refresh, hr_portal, q4_budget, mobile_app, sales_dashboard = projs

    assignments = [
        (anna, data_platform, "developer", d("2024-01-15")),
        (marc, data_platform, "lead", d("2024-01-15")),
        (carlos, data_platform, "architect", d("2024-01-15")),
        (jorge, data_platform, "developer", d("2024-02-01")),
        (ivan, data_platform, "developer", d("2024-03-01")),
        (laura, brand_refresh, "coordinator", d("2024-03-01")),
        (elena, brand_refresh, "designer", d("2024-03-15")),
        (sofia, hr_portal, "lead", d("2024-02-01")),
        (anna, mobile_app, "developer", d("2024-04-15")),
        (marc, mobile_app, "lead", d("2024-04-15")),
        (carlos, mobile_app, "architect", d("2024-04-15")),
        (ivan, mobile_app, "developer", d("2024-05-01")),
        (david, q4_budget, "analyst", d("2024-07-01")),
        (maria, q4_budget, "lead", d("2024-07-01")),
        (pablo, sales_dashboard, "developer", d("2024-05-01")),
        (clara, sales_dashboard, "analyst", d("2024-05-15")),
    ]
    for emp, proj, role, date in assignments:
        session.add(
            EmployeeProject(
                employee=emp,
                project=proj,
                role=role,
                assigned_date=date,
            )
        )

    salary_changes = [
        (anna, 48000.00, 52000.00, d("2021-03-15"), "Annual review"),
        (anna, 52000.00, 55000.00, d("2023-03-15"), "Promotion"),
        (marc, 55000.00, 58000.00, d("2020-07-01"), "Annual review"),
        (marc, 58000.00, 62000.00, d("2022-07-01"), "Promotion to lead"),
        (carlos, 60000.00, 65000.00, d("2020-11-20"), "Annual review"),
        (carlos, 65000.00, 70000.00, d("2022-11-20"), "Promotion to architect"),
        (maria, 65000.00, 70000.00, d("2019-04-01"), "Annual review"),
        (maria, 70000.00, 75000.00, d("2021-04-01"), "Promotion"),
        (david, 50000.00, 54000.00, d("2021-09-15"), "Annual review"),
        (david, 54000.00, 58000.00, d("2023-09-15"), "Annual review"),
        (jorge, 52000.00, 56000.00, d("2021-12-01"), "Annual review"),
        (jorge, 56000.00, 60000.00, d("2023-12-01"), "Promotion"),
    ]
    for emp, old, new, date, reason in salary_changes:
        session.add(
            SalaryHistory(
                employee=emp,
                old_salary=old,
                new_salary=new,
                change_date=date,
                reason=reason,
            )
        )

    session.commit()


def main() -> None:
    backend = sys.argv[1] if len(sys.argv) > 1 else "sqlite"
    database_url = SETTINGS[backend]
    print(f"Using backend: {backend}")
    print(f"Database URL: {database_url}\n")

    engine = create_engine(database_url, echo=False)
    Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)
    print("Schema created via SQLAlchemy")

    with Session(engine) as session:
        seed_data(session)

        count = session.query(Employee).count()
        print(f"Employees inserted: {count}")

        print("\nEmployees with department (via ORM):")
        for emp in session.query(Employee).order_by(Employee.last_name).all():
            print(f"  {emp.first_name} {emp.last_name} - {emp.department.name}")


if __name__ == "__main__":
    main()
